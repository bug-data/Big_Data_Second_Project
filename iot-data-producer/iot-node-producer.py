import time
import csv
import json
import sys
from kafka import KafkaProducer

NODE_ID = None
BATCH_SIZE = 5
FILENAME_FMT = 'pantheon20190612-nodo{}.csv'
TOPIC_FMT = 'node{}'

def create_producer(hostname, port):
    producerEndpointFmt = '{}:{}'
    return KafkaProducer(bootstrap_servers=producerEndpointFmt.format(hostname,port))

def send_data(producerInstance, topicName, data):
    producerInstance.send(topicName, data.encode('UTF-8'))
    print('sent a message')
    print(data)
    print()
    return
    
def send_batch_data(producerInstance, topicName, batch):
    for data in batch:
        send_data(producerInstance, topicName, data)
    return

def main():
    NODE_ID = sys.argv[1]
    filename = FILENAME_FMT.format(NODE_ID)
    topic = TOPIC_FMT.format(NODE_ID)
    
    producer = create_producer('kafka', '9092')
    print(producer)
    print()
    
    # read csv data from a single node
    with open(FILENAME_FMT.format(NODE_ID)) as csvFile:
        reader = csv.DictReader(csvFile)
        for row in reader:
            jsonRow = json.dumps(row))
            send_data(producer, topic, jsonRow)
            time.sleep(1) 


if __name__ == "__main__":
    main()
