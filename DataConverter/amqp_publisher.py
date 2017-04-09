import Queue
import pika
import time
import logging
import threading
from pika.exceptions import AMQPConnectionError, AMQPChannelError, AMQPError

import settings
from sharedqueue import common, publish_queue, acknowledge_queue

#https://github.com/pika/pika/blob/master/pika/exceptions.py
DEFAULT_PROPERTIES = pika.BasicProperties(delivery_mode=2)

class MessageAction:
    def __init__(self):
        self.helper_obj = common()

    def requeue_message(self, deliveryTag, data, properties, reason_for_requeue):
        acknowledge_queue.put({'deliveryTag': deliveryTag, 'data': data, 'RequeueRejectStatus': True, 'retryAttempt': 0, 'MessageTime': time.time()})

    def act_on_publish_failure(self, data, data_queue, deliveryTag, properties, reason_for_requeue):
        """ Logic
            If error is related to DATA
                Then, redirect data to failed queue and audit message with reason of error in message
            OTHERWISE, (Error will be related to rabbitmq connection then)
                Re-Queue message in Rabbitmq. By Enabling Re-queue option in Rabbitmq acknowledgement
        """
        try:
            if reason_for_requeue is None:
                data_queue.put(data)

            elif 'DATAERROR' in reason_for_requeue:
                publish_queue.put({'ackMsg': True,
                               'deliveryTag': deliveryTag,
                               'msgProperties': properties,
                               'data': data['data'],
                               'exchange': settings.EXCHANGE_FAILED_POSTING,
                               'routingKey': settings.FAILED_POSTING_KEY,
                               'retryAttempt': 0,
                               'MessageTime': time.time()})
                logging.error('Data Error in message. so discarding; Message redirected to fail queue.')

            else:  # Requeue message
                acknowledge_queue.put({'deliveryTag': deliveryTag, 'data': data['data'], 'RequeueRejectStatus': True, 'retryAttempt': 0, 'MessageTime': time.time()})

        except ValueError, e:
            logging.error('Data error occurred. Exception- '+str(e))
        except Exception, e:
            logging.error('Error in publishing message. Exception-'+str(e))


    def requeue_internal_message(self, data, data_queue, maxRetryTimeInSec, properties, reason_for_requeue):
        logging.debug('Message publish failed. Re-queuing message again.')
        try:
            if data is None:
                return

            if not data.has_key('retryAttempt') or data['retryAttempt'] is None:
                data['retryAttempt'] += 1
            else:
                data['retryAttempt'] = 0

            if not data.has_key('MessageTime') or data['MessageTime'] is None:  # Push the data back to queue for later processing
                data['MessageTime'] = time.time()
                data_queue.put(data)

            elif (time.time() - data['MessageTime']) < maxRetryTimeInSec:  # Push the data back to queue for later processing
                data['retryAttempt'] += 1
                data_queue.put(data)

            else:  # When all retry fails leave the message unprocessed with audit message
                self.act_on_publish_failure(data, data_queue, data['deliveryTag'], properties, reason_for_requeue)

        except ValueError, e:
            logging.error('Error in re-queueing data. Exception- '+str(e))
            self.act_on_publish_failure(data, data_queue, data['deliveryTag'], properties, 'DATAERROR')

        except Exception, e:
            logging.error('Error in re-queueing data. Exception- '+str(e))
            data_queue.put(data)


class amqp_Publisher(threading.Thread):
    PUBLISH_CONTINUE=True
    def __init__(self, parameters, message_timeout, maxRetryTimeInSec):
        threading.Thread.__init__(self)
        self.parameters=parameters
        self.message_timeout=message_timeout
        self.implClassName='BlockingConnection'  # SelectConnection
        self.connection=None
        self.channel=None
        self.maxRetryTimeInSec=maxRetryTimeInSec
        self.message_action = MessageAction()

    def close_channel(self):
        try:
            if self.channel is not None:
                logging.warn('Closing Publish Channel.')
                self.channel.close()
        except Exception, e:
            logging.warn('Error while closing rabbitmq channel. Exception-'+str(e))

    def connect(self):
        connectionClass = getattr(pika, self.implClassName)
        self.connection = connectionClass(self.parameters)
        logging.info("%s: opened connection", self.implClassName)

    def add_channel(self):
        self.channel = self.connection.channel()
        logging.info("%s: opened channel",self.implClassName)

    def enable_delivery_confirmations(self):
        self.channel.confirm_delivery()
        logging.info("%s: enabled message delivery confirmation", self.implClassName)

    def publish_message(self, data):
        properties = DEFAULT_PROPERTIES
        try:
            if not data['msgProperties'] is None:
                properties = data['msgProperties']

            status=self.channel.basic_publish(exchange=data['exchange'],
                                          routing_key=data['routingKey'],
                                          immediate=False,
                                          mandatory=True,
                                          body=data['data']
                                        )
            if not status:
                self.message_action.requeue_internal_message(data, publish_queue, self.maxRetryTimeInSec, properties, 'PUBLISH FAILED')

            if status and data['ackMsg'] and data['deliveryTag'] is not None:
                acknowledge_queue.put({'deliveryTag': data['deliveryTag'], 'data': data['data'], 'retryAttempt': 0, 'MessageTime': time.time()})
                logging.debug('Message added to acknowledge queue.')

        except (AMQPConnectionError, AMQPChannelError, AMQPError), e:
                publish_queue.put(data)
        except Exception, e:
            if e is not None and e.message != "'NoneType' object has no attribute 'send'":
                self.message_action.requeue_internal_message(data, publish_queue, self.maxRetryTimeInSec, properties, 'PUBLISHFAILED ; '+str(e.message))

    def heartbeat(self):
        try:
            self.connection.process_data_events()
        except (Exception, AMQPConnectionError), e:
            logging.warn('Error occurred . Exception-'+str(e))
            raise ValueError('Publish Heartbeat Exception-'+str(e))

    def send_msg_heartbeat(self, message_timeout):
        data = None
        last_heartbeat=time.time()
        while self.PUBLISH_CONTINUE:
            try:
                data = None
                data = publish_queue.get(timeout=message_timeout)

                self.publish_message(data)
            except (Exception, Queue.Empty), e:
                if (time.time() - last_heartbeat) > message_timeout:
                    last_heartbeat=time.time()
                    self.heartbeat()

    def start_publishing(self):
        self.maxRetryTimeInSec=self.maxRetryTimeInSec
        while self.PUBLISH_CONTINUE:
            try:
                self.connect()
                self.add_channel()
                self.enable_delivery_confirmations()

                # Initiate message publish
                self.send_msg_heartbeat(self.message_timeout)

            except Exception, e:
                logging.error('connection failure. Exception-'+e.message, exc_info=True)
                time.sleep(1)

    def run(self):
        """Run the example Publisher by connecting to RabbitMQ

        """
        self.start_publishing()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        self.PUBLISH_CONTINUE=False
        self.stop_consuming()

