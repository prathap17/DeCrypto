import dateutil.parser
import json
from websocket import create_connection, WebSocketConnectionClosedException
from kafka import KafkaProducer

class Bitfinex(object):
    def __init__(self, products=None, channels="book", producer=None):
        self.url = "wss://api.bitfinex.com/ws"