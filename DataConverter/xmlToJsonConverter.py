#!/usr/bin/env python
from Logger import event_logger
import xml.etree.ElementTree as ET
import xmltodict

class XmlToJsonConverter:
    def __init__(self):
        event_logger.info('Initialized Xml To JSON Converter.')


    def Convert_Data(self, message):
        try:
            if message == None or len(message) == 0:
                raise Exception('Message is empty')

            data = (ET.fromstring(message))
            intermediary_data = ET.tostring(data, encoding='iso-8859-1', method='xml')

            #Transform Xml data to JSON
            json_convert = xmltodict.parse(intermediary_data)
            event_logger.debug('Data conversion to json is successful.')

            return json_convert
        except Exception, e:
            event_logger.error('Error while converting data to json. Exception-'+str(e))
            raise e