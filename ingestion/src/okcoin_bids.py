import sys
from datetime import datetime
from confluent_kafka import Producer
import json
import time
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException
from config.config import KAFKA_NODES


class OkCoinBids(object):
    def __init__(self, products=None, topic="book"):
        self.url = "wss://real.okcoin.com:10440/websocket/okcoinapi"
        self.products = 'btc_usd'
        self.stop = False
        self.message_count = 0
        self.ws = None
        self.thread = None
        self.producer = Producer({
            'bootstrap.servers': ','.join(KAFKA_NODES),
            'default.topic.config': {
                'request.required.acks': 'all'
            }
        })
        print('Established Socket Connection')

        self.topic = topic
        self.probCount = 0
        self.snapshot = 0
        self.channel = ''

    def start(self):
        def _go():
            self._connect()
            self._listen()

        self.on_open()
        self.thread = Thread(target=_go())
        self.thread.start()

    def _connect(self):
        if self.topic == 'book':
            self.channel = "ok_sub_spotusd_" + self.products + "_depth"
        elif self.topic == 'trades':
            self.channel = "ok_sub_spotusd_" + self.products + "_trades"
        sub_params = "{'event':'addChannel','channel':'" + self.channel + "'}"
        self.ws = create_connection(self.url)
        self.ws.send(sub_params)

    def _listen(self):
        while not self.stop:
            try:
                msg = json.loads(self.ws.recv()[1:-1])
            except Exception as e:
                self.on_error(e)
            else:
                self.on_message(msg)

    def close(self):
        if not self.stop:
            self.on_close()
            self.stop = True
            try:
                self.thread.join()
                if self.ws:
                    self.ws.close()
            except WebSocketConnectionClosedException as e:
                self.on_error(e)

    def on_open(self):
        pass

    def on_close(self):
        pass

    def on_message(self, msg):
        try:
            if msg["channel"] == self.channel and self.snapshot == 0:
                self.snapshot = 1
                return

            bids = msg["data"]["bids"]

            if not bids:
                fmt_msg = {
                    'best_bid': str(0),
                    'number_bids': str(len(bids)),
                    'market': "okcoin",
                    'time':
                    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+0000")
                }

            else:
                fmt_msg = {
                    'best_bid': str(bids[0][0]),
                    'number_bids': str(len(bids)),
                    'market': "okcoin",
                    'time':
                    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+0000")
                }

            print(fmt_msg)
            self.probCount = 0
        except Exception as e:
            self.on_error(e)

    def on_error(self, e):
        print(e)
        self.probCount += 1
        if self.probCount > 10:
            self.close()
            exit(-1)