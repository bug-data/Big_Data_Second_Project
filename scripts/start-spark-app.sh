#!/usr/bin/env bash

sudo docker cp ../spark-app/spark-app.py spark-master:/spark-app.py
sudo docker exec -it spark-master pip install influxdb
sudo docker exec -it spark-master /spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 spark-app.py