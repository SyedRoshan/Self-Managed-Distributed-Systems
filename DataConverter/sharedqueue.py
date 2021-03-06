import Queue
import logging
import settings
import json
from datetime import datetime
import time

# Queue - Creates channel for different modules to interact
input_queue = Queue.Queue(0)
acknowledge_queue = Queue.Queue(0)
publish_queue = Queue.Queue(0)


class common:

    def __init__(self):
        logging.debug('Initialized common class')

    def extract_json_xpath(self, mydict, path):
        """
        Extract the value for the give json xpath delimited using dot '.' operator
        :param mydict: JSON dictionary
        :param path: JSON xPath
        :return: Value
        """
        elem = None
        try:
            for item in path.strip(".").split("."):
                elem = elem.get(item)
        except:
            pass

        return elem

