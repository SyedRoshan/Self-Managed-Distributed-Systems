import logging
import time
import pika
import threading

import settings
from sharedqueue import input_queue, publish_queue, acknowledge_queue

mq_connection = None
sub_channel = None
sub_consumer_tag = None

exchange_failed_posting = settings.EXCHANGE_FAILED_POSTING
failed_routing_key = settings.FAILED_POSTING_KEY


class Acknowledge(threading.Thread):
    """
    Acknowledge messages that are successfully processed and published
    """
    ACKNOWLEDGE_CONTINUE = True

    def __init__(self, maxRetryTimeInSec):
        threading.Thread.__init__(self)
        self.maxRetryTimeInSec = maxRetryTimeInSec
        logging.info('Acknowledge initiated')

    def acknowledge_message(self):
        """
        Acknowledge message to subscribed channel
        :return:
        """
        while self.ACKNOWLEDGE_CONTINUE:
            data = None
            try:
                data = acknowledge_queue.get()  # Without timeout will make thread to wait infinite until message available

                if sub_channel is not None and sub_channel.is_open and data is not None and \
                                data['deliveryTag'] is not None:
                    if data.has_key('RequeueRejectStatus') and data['RequeueRejectStatus'] is not None:
                        sub_channel.basic_reject(delivery_tag=data['deliveryTag'], requeue=data['RequeueRejectStatus'])
                    else:
                        sub_channel.basic_ack(delivery_tag=data['deliveryTag'])

            except Exception, e:
                if data['MessageTime'] is None:
                    data['MessageTime'] = time.time()
                    acknowledge_queue.put(data)  # Push the data back to queue for later processing
                elif (time.time() - data['MessageTime']) < self.maxRetryTimeInSec:
                    data['retryAttempt'] += 1
                    acknowledge_queue.put(data)  # Push the data back to queue for later processing
                else:
                    logging.error('Message discarded; since all retry for the message failed.')
                logging.warn('Message acknowledgment failed. Exception-' + str(e))

    def run(self):
        """Run the example Publisher by connecting to RabbitMQ

        """
        self.acknowledge_message()

    def stop(self):
        """
        Disable ACKNOWLEDGE_CONTINUE to stop the loop
        :return:
        """
        self.ACKNOWLEDGE_CONTINUE = False


class Amqp_Subscriber(threading.Thread):
    """
    Establishes subscription connection with Rabbitmq
    """
    SUBSCRIBE_CONTINUE = True

    def __init__(self, parameters, queue_name, prefetch, name='subscribe'):
        threading.Thread.__init__(self)
        self.parameters = parameters
        self.name = name
        self.queue_name = queue_name
        self.prefetch = prefetch

    def send_heartbeat_signal(self):
        """
        IOLoop initiated to send heartbeat signals to rabbitmq
        :return:
        """
        global mq_connection
        failcounter = 0
        try:
            if mq_connection is None:
                return False

            mq_connection.ioloop.start()
        except Exception, e:
            logging.error('Error in heartbeat to Subscribe connection. Exception- ' + str(e))
        finally:
            try:
                if mq_connection is not None:
                    mq_connection.close()
                    time.sleep(2)
                    mq_connection.ioloop.start()  # allow connection to close
            except Exception, e:
                failcounter += 1
                logging.error('IOloop error -' + e.message + ', FailRetry-' + str(failcounter))

    def initialize_connection(self):
        """
        Initialize rabbitmq connection to subscribe data
        :return:
        """
        global mq_connection
        implClassName = 'SelectConnection'
        name = self.name

        def setup_receiver(channel):
            """
            Initializes channel and binds call-back method to handle messages
            :param channel:
            :return:
            """
            global sub_channel  # Initialize global connection so that message acknowledge can be done using other thread
            global mq_connection
            try:
                def process_data(channel, method, properties, body):
                    """
                    Callback module which updates message to internal queue for processing
                    :param channel:
                    :param method:
                    :param properties:
                    :param body:
                    :return:
                    """
                    try:
                        input_queue.put({'ackMsg': True,
                                         'deliveryTag': method.delivery_tag,
                                         'msgProperties': properties,
                                         'data': body,
                                         'exchange': settings.SUBSCRIBE_EXCHANGE,
                                         'routingKey': settings.SUBSCRIBE_ROUTINGKEY,
                                         'retryAttempt': 0,
                                         'MessageTime': time.time()})

                    except (ValueError, Exception) as e:
                        logging.error('Error in json message data. Exception-' + e.message)
                        publish_queue.put({'ackMsg': True,
                                           'deliveryTag': method.delivery_tag,
                                           'msgProperties': properties,
                                           'data': body,
                                           'exchange': exchange_failed_posting,
                                           'routingKey': failed_routing_key,
                                           'retryAttempt': 0,
                                           'MessageTime': time.time()})

                if not channel.is_open:
                    raise Exception('Channel is not open, Aborting further process')

                # Assign prefetch to stream the data from rabbitmq to feed process
                channel.basic_qos(prefetch_count=self.prefetch, all_channels=False)
                sub_consumer_tag = channel.basic_consume(process_data,
                                                         queue=self.queue_name,
                                                         no_ack=False)

                sub_channel = channel  # initialize global parameter for ack

                if sub_channel.is_open:
                    logging.debug('Subscriber channel is open')

                logging.info('Initialized subscribe channel')
            except Exception, e:
                logging.error('Error while initializing subscribe channel. Exception-' + str(e))
                closeConnection(mq_connection)

        def onConnectionOpen(connection):
            logging.info("Select opening channel...")

            sub_channel = connection.channel(on_open_callback=setup_receiver)
            sub_channel.add_on_close_callback(onChannelClosed)
            # sub_channel.add_on_return_callback(onMessageReturn)

        def onChannelClosed(ch, reasonCode, reasonText):
            logging.info("Select channel closed (%s): %s", reasonCode, reasonText)
            logging.info("Closing Select connection...")
            sub_channel.connection.close()

        def onConnectionClosed(connection, reasonCode, reasonText):
            self.reset_queue()
            logging.info("Select connection closed (%s): %s", reasonCode, reasonText)

        def closeConnection(connection):
            try:
                if not connection is None:
                    connection.close()
                    logging.warn('Closing Subscribe Connection to trigger auto recovery.')

                return True
            except Exception, e:
                logging.error('Error while closing rabbitmq connection. Exception- ' + str(e))
                return False

        try:
            time.sleep(3)
            self.reset_queue()

            connectionClass = getattr(pika, implClassName)

            connection = connectionClass(
                self.parameters,
                on_open_callback=onConnectionOpen,
                on_close_callback=onConnectionClosed)

            mq_connection = connection
            logging.info('%s connected for subscribe', name)
            return True
        except Exception:
            logging.warning('%s cannot connect for subscription', name)
            return False

    def connect(self):
        global mq_connection
        while self.SUBSCRIBE_CONTINUE:
            if not self.initialize_connection():
                continue

            # Run Rabbitmq health monitor
            self.send_heartbeat_signal()

    def reset_queue(self):
        with input_queue.mutex:
            input_queue.queue.clear()

        with acknowledge_queue.mutex:
            acknowledge_queue.queue.clear()

        with publish_queue.mutex:
            publish_queue.queue.clear()
        logging.warn('Subscribe connection reset, so internal queue items cleared to avoid duplicate msg.')

    def run(self):
        """Run the example Publisher by connecting to RabbitMQ
        """
        self.connect()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.
        """
        sub_channel.close()

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame
        """
        self.close_channel()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self.sub_channel:
            self.sub_channel.basic_cancel(self.on_cancelok, sub_consumer_tag)

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
        self.SUBSCRIBE_CONTINUE = False
        self.stop_consuming()

