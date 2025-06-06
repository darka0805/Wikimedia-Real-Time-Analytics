#!/bin/bash

# Побудувати FastAPI образ
docker build -t rest-api1 .

# Запустити Redis контейнер
docker run -d \
  --network test-network \
  --name redis \
  -p 6379:6379 \
  redis:7

# Запустити FastAPI контейнер
docker run -d \
  --network test-network \
  --name rest-api1 \
  -p 8000:8000 \
  rest-api1
