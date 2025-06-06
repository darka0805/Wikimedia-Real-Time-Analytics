#!/bin/bash

docker run -d --name cassandra \
  --network test-network \
  -e CASSANDRA_START_RPC=true \
  cassandra:latest

echo "Waiting for Cassandra to initialize..."
sleep 30