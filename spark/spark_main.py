from streaming import SparkStreamConsumer

if __name__ == '__main__':
    # Kraken asset pairs for BTC, ETH, and LTC to USD prices
    asset_pairs = ['BTC-USD', 'ETH-USD']
    intervals = [1, 5, 15, 30, 60, 240, 1440, 10080, 21600]
    spread_topics = []
    ohlc_topics = []

    for asset_pair in asset_pairs:
        # Spread
        topic_name = '{}_{}'.format('Coinbase', 'Spread')
        spread_topics.append(topic_name)
    print(spread_topics)

    consumer = SparkStreamConsumer()
    consumer.consume_spreads(spread_topics)
    consumer.start_stream()