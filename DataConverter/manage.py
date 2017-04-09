#!/usr/bin/env python
import os
import sys
import pika
import threading
from functools import partial
import time

import settings
from counter import Counter
from eventTimer import TaskThread
from amqp_publisher import amqp_Publisher
from dataConverter import DataConverter
from amqp_subscriber import Amqp_Subscriber, Acknowledge
from sharedqueue import input_queue, publish_queue, acknowledge_queue
from Logger import event_logger, stats_logger, TimeLog  

acknowledge_Threads = []
publish_Threads = []
data_Converter_Threads = []
stats_thrd = None


def reset_backlog(input_queue):
    try:
        with input_queue.mutex:
            input_queue.queue.clear()
            event_logger.warn('Items in Input queue have been cleared.')
    except Exception, e:
        event_logger.error('Queue reset failed. Exception - '+e.message)


def clear_queue_items():
    try:
        reset_backlog(input_queue)

        global stats_thrd
        if stats_thrd is not None:
            stats_thrd.do_task()
    except Exception, e:
        event_logger.error('Error while clearing items. Exception-' + e.message)


def getPikaConnectionParameters(config):
    """
  :returns: instance of pika.ConnectionParameters for the AMQP broker (RabbitMQ
  most likely)
  """
    credentials = pika.PlainCredentials(config['USER'], config['PASSWORD'])

    return pika.ConnectionParameters(host=config['HOST'], virtual_host=config['VHOST'],heartbeat_interval=config['HEARTBEAT'],
                                   credentials=credentials, socket_timeout=3)


def initiate_subscribe():
    subscribe_MaxRetryTimeInSec = 1800
    # Subscriber thread initialize to fetch message from rabbitmq and stream in given queue
    queue_name = settings.SUBSCRIBE_QUEUE
    prefetch = settings.INPUT_RABBIT['PREFETCH']

    # Initialize rabbitmq connection for subscription
    sub_param = getPikaConnectionParameters(settings.INPUT_RABBIT)

    # Subscribe thread
    sub = Amqp_Subscriber(sub_param, queue_name, prefetch)
    sub.daemon = True
    sub.start()

    time.sleep(2)


def initiate_publish():
    maxPublishRetryTimeInSec = 1800
    pub_param = getPikaConnectionParameters(settings.OUTPUT_RABBIT)

    heartbeat_freq = 25
    if settings.OUTPUT_RABBIT['HEARTBEAT'] is not None and settings.OUTPUT_RABBIT['HEARTBEAT'] > 1:
        heartbeat_freq = int(settings.OUTPUT_RABBIT['HEARTBEAT']/4)

    #Publish thread
    for iterator in range(settings.PUBLISH_THREAD):  # Spawn configured number of worker threads for parallelism
        pub = None
        pub_thrd = None
        pub = amqp_Publisher(pub_param, heartbeat_freq, maxPublishRetryTimeInSec)
        pub.daemon = True
        publish_Threads.insert(iterator, pub)
        publish_Threads[iterator].start()


def initiate_process():
    process_MaxRetryTimeInSec = 1800

    # Process initiator thread
    for iterator in range(0, settings.THREAD_COUNT, 1):  # Spawn configured number of worker threads for parallelism
        prc = None
        process_thrd = None
        prc = DataConverter(str(iterator))
        process_thrd = threading.Thread(target=partial(prc.monitor_process_data, input_queue, publish_queue, process_MaxRetryTimeInSec))
        process_thrd.daemon = True

        data_Converter_Threads.insert(iterator, process_thrd)
        data_Converter_Threads[iterator].start()
        event_logger.debug('Initiated patch thread. ThreadNumber-' + str(iterator))
        time.sleep(0.2)
        #iterator += 1
    event_logger.info('Message process initiated')


def initiate_acknowledge():
    ack_MaxRetryTimeInSec = 1800

    #Acknowledge thread
    for iterator in range(settings.ACKNOWLEDGE_THREAD):  # Spawn configured number of worker threads for parallelism
        ack = None
        ack_thrd = None
        ack = Acknowledge(ack_MaxRetryTimeInSec)
        ack.daemon = True
        acknowledge_Threads.insert(iterator, ack)
        acknowledge_Threads[iterator].start()


def main():
    try:
        stats = Counter()
        stats.reset_counters()  # Reset will initialize time settings and counters

        event_logger.info('Initiating Data Conversion')

        #Initialize Publish
        initiate_publish()

        #Initialize Process
        initiate_process()

        #Initalize Subscribe
        initiate_subscribe()

        #Initialize Acknowledge
        initiate_acknowledge()

        global stats_thrd
        stats_thrd = TaskThread(settings.STATS_EVENT_TIME_SEC)  # Instantiate a thread to track message counters
        stats_thrd.daemon = True
        stats_thrd.start()
        event_logger.info('Statistics logging thread initiated')

        while True:  # Maintain main thread alive
            time.sleep(30)  # This will never be triggered; since acknowledge module has infinite loop

    except KeyboardInterrupt, k:
        clear_queue_items()
        event_logger.warn('Keyboard Interrupt; Aborting process. ' + str(k))
        print('Keyboard Interrupt. Aborting process')

    except Exception, e:
        clear_queue_items()
        event_logger.error('Error occurred in main thread. Exception-' + str(e))
        print('Error occurred in main thread. Exception-' + str(e))


if __name__ == '__main__':
    print '[*] Initiating message process. To exit press CTRL+C'
    main()


event_logger.info("Data Converter Shutdown Complete.")
