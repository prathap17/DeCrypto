import sys
from datetime import datetime
from kafka import KafkaProducer
import json
import time
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException


class BitfinexProducer(object):
    def __init__(self, products=None, channels="book", producer=None,addr='0.0.0.0', topic="trades"):
        self.url = "wss://api.bitfinex.com/ws"
        self.products = ['btcusd','ltcusd','bchusd','ethusd']
        self.channels = channels
        self.stop = False
        self.message_count = 0
        self.ws = None
        self.thread = None
        self.producer = KafkaProducer(bootstrap_servers=addr,
                                      key_serializer=lambda v: v.encode('ascii'),
                                      value_serializer=lambda v: json.dumps(v).encode('ascii'))
        self.topic = topic
        self.probCount = 0
        self.snapshot = 0

    def start(self):
        def _go():
            self._connect()
            self._listen()
        
        self.on_open()
        self.thread = Thread(target=_go())
        self.thread.start()
    
    def _connect(self):
        sub_params = {'event': 'subscribe', 'channel': self.channels, 'pair': self.products, 'prec': "P0", 'freq': "F0"}
        self.ws = create_connection(self.url)
        self.ws.send(json.dumps(sub_params))

    def _listen(self):
        while not self.stop:
            try:
                if int(time.time() % 30) == 0:
                    # Set a 30 second ping to keep connection alive
                    self.ws.ping("keepalive")
                msg = json.loads(self.ws.recv())
            except Exception as e:
                self.on_error(e)
            else:
                self.on_message(msg)

    def close(self):
        if not self.stop:
            if self.channels == "heartbeat":
                self.ws.send(json.dumps({"type": "heartbeat", "on": False}))
            self.on_close()
            self.stop = True
            self.thread.join()
            try:
                if self.ws:
                    self.ws.close()
            except WebSocketConnectionClosedException as e:
                self.on_error(e)

    def on_open(self):
        pass

    def on_close(self):
        pass

    def on_message(self, msg):
#        print(msg)
        if 'hb' in msg or not isinstance(msg,list):
            return
        
        try:
            # try if 'type'=='snapshot'
            if self.snapshot == 0:
                self.snapshot = 1
                return
        
            if 'tu' not in msg:
                return
            fmt_msg = {'price': str(msg[5]),
                        'amount': str(abs(msg[6])),
                        'time': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+0000")}
            topic = self.topic + '-' + self.products.lower()
        
            fmt_msg['market'] = "bitfinex"

            print(topic)
#            print(msg)
#            print(fmt_msg)
            self.producer.send(topic, key=topic, value=fmt_msg) #***************
            self.probCount = 0
        except Exception as e:
            self.on_error(e)

    def on_error(self, e):
        print(e)
        self.probCount += 1
        if self.probCount > 10:
            self.close()
            exit(-1)


