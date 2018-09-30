# -*- coding: utf-8 -*-

import os
import sys
import dateutil.parser
import json
from websocket import create_connection, WebSocketConnectionClosedException
import cbpro 
from confluent_kafka import Producer
from config.config import KAFKA_NODES
root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')
import ccxt  

# return up to ten bidasks on each side of the order book stack

class Cex():
    def __init__(self, products, data):
        self.limit=100
        self.products=products
        self.data=data
        self.producer = Producer({'bootstrap.servers': ','.join(KAFKA_NODES), 'default.topic.config': { 'request.required.acks': 'all' }})
        print('Established Socket Connection')
        
    
    def produce(self):
        
        def delivery_report(err, k_msg):
            print('hi')
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print(('Message delivery failed: {}'.format(err)))
            else:
                print(('Message delivered to {} [{}] - {}'.format(
                    k_msg.topic(), k_msg.partition(), self.products)))

        
        if 'timestamp' in self.data:  # timestamp
            
            self.data['product'] = self.products 

            message = json.dumps(self.data)
            

                # feed to kafka
            topic = 'Cex'
            self.producer.poll(0)
            self.producer.produce(
                    topic,
                    message.encode('utf-8'),
                    key=self.products,
                    callback=delivery_report)