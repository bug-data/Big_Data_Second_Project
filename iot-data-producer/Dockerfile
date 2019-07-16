FROM python:3

COPY dummy-producer.py iot-node-producer.py iot-weather-station-producer.py start-all-producers.sh data/* ./

RUN pip install kafka-python && touch test.txt

ENTRYPOINT ["tail", "-f", "test.txt"]