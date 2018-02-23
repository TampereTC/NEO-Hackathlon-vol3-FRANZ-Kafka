#!/usr/bin/python

from kafka import KafkaConsumer

from kafka import KafkaProducer

import re


consumer = KafkaConsumer(bootstrap_servers='kafka:9092')

producer = KafkaProducer(bootstrap_servers='kafka:9092')



consumer.subscribe(pattern="novel.*")




print "Ready to rekafka"


for message in consumer:
	newmessage = re.sub("K([, ])", "NEO-K\\1", message.value)
	#newmessage = message.value.replace('K ', 'NEO-K ')

	print newmessage

	producer.send('rekafka' , newmessage)

