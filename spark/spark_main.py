from streaming import SparkStreamConsumer

if __name__ == '__main__':

    consumer = SparkStreamConsumer()
    consumer.consume(['Coinbase', 'Cex'])
    consumer.start_stream()