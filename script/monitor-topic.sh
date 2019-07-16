#!/usr/bin/env bash

echo $1
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $1 --from-beginning