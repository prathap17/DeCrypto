import dateutil.parser
import json
from websocket import create_connection, WebSocketConnectionClosedException
import cbpro 
from confluent_kafka import Producer
from config.config import KAFKA_NODES

#print(KAFKA_NODES)


class CoinbasePro(cbpro.WebsocketClient):
    def on_open(self):
        self.url = "wss://ws-feed.pro.coinbase.com/"  # websocket url for the coinbasepro
        self.products = ["BTC-USD", "ETH-USD", "LTC-USD",
                         "BCH-USD"]  # coinbase supports four coins
        self.type = 'ticker'
        self.producer = Producer({'bootstrap.servers': ','.join(KAFKA_NODES), 'default.topic.config': { 'request.required.acks': 'all' }})
        print('Established Socket Connection')
        

    def on_message(self, msg):
        
        def delivery_report(err, k_msg):
            
            """ Called once for each message produced to indicate delivery result.
                Triggered by poll() or flush(). """
            if err is not None:
                print(('Message delivery failed: {}'.format(err)))
            else:
                print(('Message delivered to {} [{}] - {}'.format(
                    k_msg.topic(), k_msg.partition(), msg['product_id'])))

        if 'time' in msg:  # timestamp
            
            asset_pair = msg['product_id']
            timestamp = dateutil.parser.parse(msg['time']).timestamp()
            data = [
                    timestamp, msg['best_bid'], msg['best_ask'], asset_pair,
                    asset_pair
                ]
            message = json.dumps(data)
            #print(message)

                # feed to kafka
            topic = 'Coinbase'
            self.producer.poll(0)
            self.producer.produce(
                    topic,
                    message.encode('utf-8'),
                    key=asset_pair,
                    callback=delivery_report)

    # Overwriting private method to allow for subscribing to the 'ticker' channel
    def _connect(self):
        if self.products is None:
            self.products = ["BTC-USD"]
        elif not isinstance(self.products, list):
            self.products = [self.products]

        if self.url[-1] == "/":
            self.url = self.url[:-1]

        self.ws = create_connection(self.url)

        self.stop = False

        if self.type == "heartbeat":
            sub_params = {
                'type': 'subscribe',
                "channels": [{
                    "name": "heartbeat",
                    "product_ids": self.products
                }]
            }
            self.ws.send(json.dumps(sub_params))
        elif self.type == 'ticker':
            sub_params = {
                'type': 'subscribe',
                "channels": [{
                    "name": "ticker",
                    "product_ids": self.products
                }]
            }
            self.ws.send(json.dumps(sub_params))
        else:
            sub_params = {'type': 'subscribe', 'product_ids': self.products}
            self.ws.send(json.dumps(sub_params))