#!/bin/bash
docker stop $(docker ps -aq) && docker rm $(docker ps -aq) && docker rmi -f $(docker images -q)
docker network rm test-network 