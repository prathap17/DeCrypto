from streaming import SparkConsumer

if __name__ == '__main__':

    consumer = SparkConsumer()
    consumer.consume(['bids','asks'])
    consumer.start_stream()