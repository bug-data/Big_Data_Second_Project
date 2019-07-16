FROM python:3

COPY dummy-producer.py .
COPY iot-node-producer.py .
COPY data/pantheon20190612-nodo8.csv .

RUN pip install kafka-python && touch test.txt

ENTRYPOINT ["tail", "-f", "test.txt"]