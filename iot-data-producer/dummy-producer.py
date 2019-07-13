import time
from kafka import KafkaProducer

time.sleep(30)
producer = KafkaProducer(bootstrap_servers='kafka:9092')
print(producer)
print()
for i in range(100):
	future = producer.send('test', b'some_message_bytes')
	time.sleep(10)
	print('sent a message')
	print(i)