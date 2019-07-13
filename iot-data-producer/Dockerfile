FROM python:3

COPY dummy-producer.py .

RUN pip install kafka-python && touch test.txt

ENTRYPOINT ["tail", "-f", "test.txt"]