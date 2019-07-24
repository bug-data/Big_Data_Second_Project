import time
import csv
import json
import sys
from kafka import KafkaProducer

BATCH_SIZE = 5


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


def remove_empty_values(jsonData):
    cleanJson = {}
    for key, value in jsonData.items():
        if value and (value != '{}' or value != "{}"):
            cleanJson[key] = value
    return cleanJson

def main():
    filename = 'pantheon20190612-stazione.csv'
    topic = 'weather'
    producer = create_producer('kafka', '9092')
    print(producer)
    print()
    

    with open(filename) as csvFile:
        reader = csv.DictReader(csvFile, delimiter=';')
        for row in reader:
            jsonString = json.dumps(row)
            jsonCleanData = remove_empty_values(json.loads(jsonString))
            jsonCleanData = json.dumps(jsonCleanData))
            send_data(producer, topic, jsonCleanData)
            time.sleep(1) 


if __name__ == "__main__":
    main()
