#!usr/bin/env python3
#   Copyright 2019 BIBA-Bremer Institut für Produktion und Logistik GmbH, IoTFabLab
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#        http://www.apache.org/licenses/LICENSE-2.0
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#   
#   V1.0.0 - 10/10/2019 Updated by Shantanoo Desai <des@biba.uni-bremen.de>

import logging
import toml
from pymongo import MongoClient
from influxdb import InfluxDBClient
import paho.mqtt.client as mqtt

import urllib3
urllib3.disable_warnings()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

mqtt_client = mqtt.Client()
mqtt_client.enable_logger(logger)

logger.info('Reading Configuration File')
with open('config.toml') as conf_file:
    CONFIG = toml.load(conf_file)

logger.info('Setting Up Document DB Client')
doc_cl = CONFIG['DocumentDB']
logger.debug('Connecting to host:{} @ port:{}'.format(doc_cl['host'], doc_cl['port']))

try:
    document_db = MongoClient(host=doc_cl['host'],
                                port=doc_cl['port'],
                                username=doc_cl['credentials']['username'],
                                password=doc_cl['credentials']['password'])

    logger.debug('Obtaining {} Database for stored Sites'.format(doc_cl['dbName']))
    sites_db = document_db[doc_cl['dbName']]

    logger.debug('Obtaining {} Collection of all Sites'.format(doc_cl['collection']))
    sites = sites_db[doc_cl['collection']]

except Exception as e:
    logger.exception(e)
    document_db.close()

logger.info('Setting up Sensor DB Client')
sen_cl = CONFIG['SensorDB']
logger.debug('Connecting to host:{} @ port:{}'.format(sen_cl['host'], sen_cl['port']))

try:
    sensor_db = InfluxDBClient(host=sen_cl['host'],
                                port=sen_cl['port'],
                                ssl=sen_cl['credentials']['ssl'],
                                verify_ssl=sen_cl['credentials']['verify_ssl'],
                                username=sen_cl['credentials']['username'],
                                password=sen_cl['credentials']['password'],
                                database=sen_cl['dbName'])
except Exception as e:
    logger.exception(e)
    sensor_db.close()

def get_site_topics():
    """ Function to return topics to subscribe from the stored Sites in Document DB """
    topics_to_subscribe = [] # List of Tuples: (topic_name, qos)
    for each_site in sites.find():
        topics_to_subscribe.append((each_site['topic'], 0)) # Default QoS:0
    return topics_to_subscribe

def send_sensor_data(line_proto):
    """Function to send data Sensor DB using InfluxDB Line Protocol"""
    sensor_db.write_points(str(line_proto), database=sen_cl['dbName'], time_precision='s', protocol=u'line')

## 
# MQTT Client Callback Functions
##
def on_connect(client, obj, flags, rc):
    """Callback Function: on_connect for MQTT Client"""
    if rc == 0:
        logger.info('MQTT Client Connected. rc = {}'.format(rc))
    else:
        logger.info('MQTT Client Not Connected with rc = {}'.format(rc))

def on_subscribe(client, obj, mid, granted_qos):
    """Callback Function: on_subscribe for MQTT Client"""
    logger.debug('on_subscribe: Granted QoS = {}'.format(granted_qos))

def on_message(client, obj, msg):
    """Callback Function: on_message for MQTT Client. Parsing Logic for incoming data"""

    # Incoming Topic Format: <company_name>/<site_name>/<country_code>/<city>/<sensorMAC>/<sensorType>
    topic_levels = msg.topic.split('/')

    # Add the Topic Levels as Tags for InfluxDB Line Protocol
    company = "company=" + topic_levels[0] + ','
    site = "site=" + topic_levels[1] + ','
    country = "country=" + topic_levels[2] + ','
    city = "city=" + topic_levels[3] + ','
    sensorID = "sID=" + topic_levels[4] + ','

    # Incoming Payload Format: InfluxDB Line Protocol
    incoming_payload = msg.payload.decode('utf-8').split(' ')

    ## Add BizLocation based on Meta-Data stored in Sites Collection

    # find particular site based on Topic Levels
    site_info = sites.find_one({
        "siteName": topic_levels[1],
        "city": topic_levels[3],
        "company": topic_levels[0]
        })
    
    # get Sensor mapping
    sensor_map = site_info['sensors']

    for map in sensor_map:
        if map['mac'] == topic_levels[4]:
            bizLocation = "bizLocation=" + map['bizLocation']

            # Add bizLocation and other meta-data as tags to Line Protocol
            incoming_payload[0] += ',' + company + site + country + city + sensorID + bizLocation
    
    incoming_payload = ' '.join(incoming_payload)
    logger.debug('Line Protocol to Write: ' + incoming_payload)
    send_sensor_data(incoming_payload)

if __name__ == "__main__":
    try:
        logger.info('Setting Up MQTT Client')
        mqtt_client.on_message = on_message
        mqtt_client.on_connect = on_connect
        mqtt_client.on_subscribe = on_subscribe
        mqtt_client.connect(CONFIG['Broker']['host'], port=CONFIG['Broker']['port'])

        logger.info('Getting Topics to Subscribe from Sites Collection')
        topics = get_site_topics()
        logger.debug('Topics, QoS: {}'.format(topics))
        mqtt_client.subscribe(topics, 0)
        mqtt_client.loop_forever()
    
    except Exception as e:
        logger.error('Exception Trigger')
        logger.exception(e)
        mqtt_client.loop_stop()
        document_db.close()
        sensor_db.close()
    except KeyboardInterrupt:
        logger.error('CTRL+C Pressed')
        mqtt_client.loop_stop()
        document_db.close()
        sensor_db.close()
        