#!/usr/bin/env bash

docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $1 --from-beginning