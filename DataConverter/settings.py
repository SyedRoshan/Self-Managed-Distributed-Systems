# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os
import random

import logging,logging.handlers

BASE_DIR = os.path.dirname(os.path.dirname(__file__))


STATS_EVENT_TIME_SEC = 60

THREAD_COUNT = 1
PUBLISH_THREAD=1
ACKNOWLEDGE_THREAD=1

BROKER_CONNECTION_TIMEOUT  = 10

EXCHANGE_FAILED_POSTING = 'ExchangeFailedPosting'
FAILED_POSTING_KEY = 'FailedPosting'

SUBSCRIBE_QUEUE ='0_DataConverter_Xml'
SUBSCRIBE_EXCHANGE = '0_Exchange_DataConverter'
SUBSCRIBE_ROUTINGKEY = 'xml'

PUBLISH_EXCHANGE = '0_Exchange_JSONResult'
PUBLISH_ROUTINGKEY = 'json'

#RabbitMQ server to get postings for classification
INPUT_RABBIT = {'HOST': 'localhost',
                'USER': 'test',
                'PASSWORD': 'test',
                'HEARTBEAT': 60,
                'PREFETCH': 1,
                'PORT': 5672,
               'VHOST':'/'}

#RabbitMQ server to send postings destined for Nova
OUTPUT_RABBIT = {'HOST': 'localhost',
               'USER': 'test',
               'PASSWORD': 'test',
               'HEARTBEAT': 60,
               'PORT': 5672,
               'VHOST':'/',
               'PUB_CHANNEL_COUNT':1}

LogId = random.randrange(1000, 9999, 2)
LOG_FILENAME = 'D:\\TestData\\0_NOVA\\Log\\DataConverter\\DataConverter_'+str(LogId)+'.log'
STAT_LOG_FILENAME = 'D:\\TestData\\0_NOVA\\Log\\DataConverter\\DC_Status_'+str(LogId)+'.log'
Time_LOG_FILENAME = 'D:\\TestData\\0_NOVA\\Log\\DataConverter\\DC_TIME_Status_'+str(LogId)+'.log'

LOG_FORMAT = ('%(asctime)s|%(levelname)s|%(funcName)s| %(message)s')
STATS_LOG_FORMAT = ('%(asctime)s|%(message)s')
TIME_LOG_FORMAT = ('%(message)s')
TIME_LOG_LEVEL = logging.WARN
LOG_LEVEL = logging.INFO
MAX_LOG_BYTES = 5000000
MAX_OLD_LOGS = 1000