#!/bin/bash
docker build -t producer .
docker run --name producer --network test-network producer
