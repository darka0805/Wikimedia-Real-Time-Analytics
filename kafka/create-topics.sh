#!/bin/bash
docker run -it --rm --network test-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:3.3.2 kafka-topics.sh --create  --bootstrap-server kafka-server:9092 --replication-factor 1 --partitions 3 --topic input 

docker run -it --rm --network test-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:3.3.2 kafka-topics.sh --create  --bootstrap-server kafka-server:9092 --replication-factor 1 --partitions 3 --topic processed
