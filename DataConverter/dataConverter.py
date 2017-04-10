#!/usr/bin/env python
import os
import sys
import pika
import threading
import logging
import time
import json
import settings
from counter import Counter
from sharedqueue import publish_queue
from amqp_publisher import DEFAULT_PROPERTIES, MessageAction
from Logger import event_logger
from xmlToJsonConverter import XmlToJsonConverter


stats = Counter()
class DataConverter:
    def __init__(self, thread_name):
        self.thread_name=thread_name
        self.initialize_converter()
        event_logger.info('Initialized Data Conversion. Thread-#'+str(thread_name))


    def initialize_converter(self):
        self.data_converter = XmlToJsonConverter()


    def monitor_process_data(self, input_data_queue, output_data_queue, maxRetryTimeInSec):
        global lock
        msg_action = MessageAction()
        data=None
        while True:
            try:
                json_data=None
                data = input_data_queue.get()
                stats.start_stopwatch()

                logging.debug('got data to process')
                json_data = self.data_converter.Convert_Data(data['data'])

                if json_data is None:
                    self.failed_data(data, 'JSON parsing failed')
                    stats.increment_failure()
                    stats.pause_stopwatch()
                    continue

                stats.increment_success()
                #Publish data
                output_data_queue.put({'ackMsg': True,
                                   'deliveryTag': data['deliveryTag'],
                                   'msgProperties': data['msgProperties'],
                                   'data': str(json.dumps(json_data)),
                                   'exchange': settings.PUBLISH_EXCHANGE,
                                   'routingKey': settings.PUBLISH_ROUTINGKEY,
                                   'retryAttempt': 0,
                                   'MessageTime': time.time()})

                data= None
                stats.pause_stopwatch()
            except Exception, e:
                logging.error('submission failed. '+e.message, exc_info=True)
                properties = None
                if data is not None and data.has_key('msgProperties'):
                    properties=data['msgProperties']
                else:
                    properties=DEFAULT_PROPERTIES

                if data is None:
                    stats.increment_failure()
                    msg_action.requeue_internal_message(data, input_data_queue, maxRetryTimeInSec, properties, 'DATAERROR ; '+str(e.message))


    def failed_data(self, data, exception_message):
        try:
            publish_queue.put({'ackMsg': True,
                                       'deliveryTag': data['deliveryTag'],
                                       'msgProperties': data['msgProperties'],
                                       'data': data['data'],
                                       'exchange': settings.EXCHANGE_FAILED_POSTING,
                                       'routingKey': settings.FAILED_POSTING_KEY,
                                       'retryAttempt': 0,
                                       'MessageTime': time.time()})
        except Exception, e:
            event_logger.error('Exception-'+str(e))
            return False