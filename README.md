Customer Event Generator - Kafka and Spark
===

Events generator produces random customers data by Kafka producer then get consumed by a spark streaming context and does some alterations.

Run
---
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 spotify/kafka
start customer-sanitize CustomerStreaming.java
start customersKafka CustomerProducer.java