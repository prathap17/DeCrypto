from streaming import SparkStreamConsumer

if __name__ == '__main__':

    consumer = SparkStreamConsumer()
    consumer.consume_spreads(['Coinbase'])
    consumer.start_stream()