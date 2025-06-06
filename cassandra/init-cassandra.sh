#!/bin/bash

docker cp "/mnt/c/Downloads/Project/Project/cassandra/create-tables.cql" cassandra:/create-tables.cql
docker exec -it cassandra cqlsh -f /create-tables.cql
