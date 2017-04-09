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
    """
    Class will allow to take decision on message with unexpected behavior due to data level or environment issues
    """

    def __init__(self):
        self.helper_obj = common()

    def requeue_message(self, deliveryTag, data, properties, reason_for_requeue):
        """
        Requeue message in queue so that same data can be processed later.
        :param deliveryTag:
        :param data:
        :param properties:
        :param reason_for_requeue:
        :return:
        """
        acknowledge_queue.put({'deliveryTag': deliveryTag, 'data': data, 'RequeueRejectStatus': True, 'retryAttempt': 0,
                               'MessageTime': time.time()})

    def act_on_publish_failure(self, data, data_queue, deliveryTag, properties, reason_for_requeue):
        """
        Decides action to be taken on message when publish to rabbit queue is failed.
        Logic
            If error is related to DATA
                Then, redirect data to failed queue and audit message with reason of error in message
            OTHERWISE, (Error will be related to rabbitmq connection then)
                Re-Queue message in Rabbitmq. By Enabling Re-queue option in Rabbitmq acknowledgement
        :param data:
        :param data_queue:
        :param deliveryTag:
        :param properties:
        :param reason_for_requeue:
        :return:
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
                acknowledge_queue.put({'deliveryTag': deliveryTag, 'data': data['data'], 'RequeueRejectStatus': True,
                                       'retryAttempt': 0, 'MessageTime': time.time()})

        except ValueError, e:
            logging.error('Data error occurred. Exception- '+str(e))
        except Exception, e:
            logging.error('Error in publishing message. Exception-'+str(e))

    def requeue_internal_message(self, data, data_queue, maxRetryTimeInSec, properties, reason_for_requeue):
        """
        Validate message arrival time and requeue within application to attempt processing
        :param data:
        :param data_queue:
        :param maxRetryTimeInSec:
        :param properties:
        :param reason_for_requeue:
        :return:
        """

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
            data_queue.put(data)  # Requeue message within internal queue to attempt processing again.


class amqp_Publisher(threading.Thread):
    """
    Publishes message to configured rabbitmq and maintains active connection to rabbitmq
    """
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
        """
        Close publish channel
        :return:
        """
        try:
            if self.channel is not None:
                logging.warn('Closing Publish Channel.')
                self.channel.close()
        except Exception, e:
            logging.warn('Error while closing rabbitmq channel. Exception-'+str(e))

    def connect(self):
        """
        Establish connection to rabbitmq
        :return:
        """

        connectionClass = getattr(pika, self.implClassName)
        self.connection = connectionClass(self.parameters)
        logging.info("%s: opened connection", self.implClassName)

    def add_channel(self):
        """
        Open channel to the existing rabbitmq connection
        :return:
        """

        self.channel = self.connection.channel()
        logging.info("%s: opened channel",self.implClassName)

    def enable_delivery_confirmations(self):
        """
        Confirm delivery will always ensure message published is written to disk
        :return:
        """

        self.channel.confirm_delivery()
        logging.info("%s: enabled message delivery confirmation", self.implClassName)

    def publish_message(self, data):
        """
        Publish message to desired rabbitmq based on exchange and routingkey embedded within message
        :param data:
        :return:
        """

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
            if not status:  # In case of failure, retry message publish
                self.message_action.requeue_internal_message(data, publish_queue, self.maxRetryTimeInSec, properties, 'PUBLISH FAILED')

            if status and data['ackMsg'] and data['deliveryTag'] is not None:
                acknowledge_queue.put({'deliveryTag': data['deliveryTag'], 'data': data['data'], 'retryAttempt': 0,
                                       'MessageTime': time.time()})
                logging.debug('Message added to acknowledge queue.')

        except (AMQPConnectionError, AMQPChannelError, AMQPError), e:
                publish_queue.put(data)  # Requeue message for later retry
        except Exception, e:
            if e is not None and e.message != "'NoneType' object has no attribute 'send'":
                self.message_action.requeue_internal_message(data, publish_queue, self.maxRetryTimeInSec, properties,
                                                             'PUBLISHFAILED ; '+str(e.message))

    def heartbeat(self):
        """
        Signals Rabbitmq with heartbeat message to ensure active connection.
        :return:
        """
        try:
            self.connection.process_data_events()
        except (Exception, AMQPConnectionError), e:
            logging.warn('Error occurred . Exception-'+str(e))
            raise ValueError('Publish Heartbeat Exception-'+str(e))

    def send_msg_heartbeat(self, message_timeout):
        """
        Publish message and heartbeat signals
        :param message_timeout:
        :return:
        """
        data = None
        last_heartbeat=time.time()
        while self.PUBLISH_CONTINUE:
            try:
                data = None
                data = publish_queue.get(timeout=message_timeout)  # Fetch data from internal queue

                self.publish_message(data)  # Publish message
            except (Exception, Queue.Empty), e:
                if (time.time() - last_heartbeat) > message_timeout:
                    last_heartbeat=time.time()
                    self.heartbeat()

    def start_publishing(self):
        """
        Initialize rabbitmq connection and data publishing
        :return:
        """
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
        """
        Run the example Publisher by connecting to RabbitMQ
        :return:
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

