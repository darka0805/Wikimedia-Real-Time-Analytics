#!/bin/bash
docker run --rm -it --network test-network \
    --name spark-submit \
    --user root \
    -v /mnt/c/Downloads/Project/Project:/opt/app \
    bitnami/spark:3 /bin/bash
