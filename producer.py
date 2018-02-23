#!/usr/bin/python

from kafka import KafkaProducer
import sys
import time

def python_kafka_producer_performance():
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    count = int(sys.argv[2])
    message = sys.argv[1]
    producer_start = time.time()
    topic = 'mytopic'
    for i in range(count):
        producer.send(topic, '%s %i' % (message, i))
        
    producer.flush() # clear all local buffers and produce pending messages
        
    return time.time() - producer_start

print 'Time used producing messages %f seconds' % python_kafka_producer_performance()
