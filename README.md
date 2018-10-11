# DeCrypto
Demystify crypto trading. 

# Description

Over the most recent years, digital assets have turned into another type of monetary resources intended to fill in as a medium of trade. Based on this popularity various crypto exchanges have built up.

The blockchain is the underlying technologies for the cryptocurrencies. Decentralization is the major motto of the blockchain. But the exchanges are not decentralized. Each exchange has their own set of prices for trading.

The objective of the DeCrypto is to create a centralized hub for these exchanges and suggest the best exchange for the trade

# Pipeline

Data from different  APIs from the markets are ingested to several Kafka producers and categorized into different queues according to its nature.

Spark Streaming consumes all queues from Kafka and calculates the best exchange for trading.

<img width="1260" alt="screen shot 2018-10-11 at 2 46 00 pm" src="https://user-images.githubusercontent.com/31057560/46826660-8b29db80-cd64-11e8-951f-34b992d66fc4.png">
