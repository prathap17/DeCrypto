from streaming import SparkStreamConsumer

if __name__ == '__main__':

    consumer = SparkStreamConsumer()
    consumer.consume(['Coinbase'])
    consumer.start_stream()