import dateutil.parser
import json
from confluent_kafka import Producer
from config.config import KAFKA_NODES
from datetime import datetime

""" 
    The Bitstamp class fetches data from Bitsamp api and formats the data.
    And push data to kafka topic

"""


class BitStamp():
    def __init__(self, products, data):
        self.limit = 100
        self.products = products
        self.data = data
        self.producer = Producer({
            'bootstrap.servers': ','.join(KAFKA_NODES),
            'default.topic.config': {
                'request.required.acks': 'all'
            }
        })
        print('Established Socket Connection')

    def produce(self):
        
        def delivery_report(err, k_msg):
             
            # triggers delivery report  by poll() or flush()
            
            if err is not None:
                print(('Message delivery failed: {}'.format(err)))
            else:
                print(('Message delivered to {} [{}] - {}'.format(
                    k_msg.topic(), k_msg.partition(), self.products)))

        if 'timestamp' in self.data:  

             # data formatting
            data = {
                'bids': (self.data['bids'][0][0]),
                'len_bids': (abs(len(self.data['bids']))),
                'asks': (self.data['asks'][0][0]),
                'len_asks': (abs(len(self.data['asks']))),
                'product': self.products.replace("/", "-"),
                'time': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+0000")
            }

            data['market'] = "Bitstamp"
            message = json.dumps(data)
            
            # push t0 kafka topic

            topic = 'bids'
            self.producer.poll(0)
            self.producer.produce(
                topic,
                message.encode('utf-8'),
                key=self.products.replace('/', '-'),
                callback=delivery_report)