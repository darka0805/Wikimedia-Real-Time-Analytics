#!/bin/bash

docker build -t rest-api2 .
# Запустити Redis контейнер


docker run -d --network test-network -p 8001:8000 --name rest-api2 rest-api2