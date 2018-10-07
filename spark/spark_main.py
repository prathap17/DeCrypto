from streaming import SparkStreamConsumer

if __name__ == '__main__':

    consumer = SparkStreamConsumer()
    consumer.consume(['test'])
    consumer.start_stream()