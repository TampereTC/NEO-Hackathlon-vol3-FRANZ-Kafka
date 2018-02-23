#!/usr/bin/python

from kafka import KafkaConsumer
import time
import sys

def python_kafka_consumer_performance():
    topic = 'mytopic'
    msg_count = int(sys.argv[1])

    consumer = KafkaConsumer(
        bootstrap_servers='kafka:9092',
        auto_offset_reset = 'earliest', # start at earliest topic
        group_id = None # do no offest commit
    )
    msg_consumed_count = 0
            
    consumer_start = time.time()
    consumer.subscribe([topic])
    for msg in consumer:
        msg_consumed_count += 1

        #print msg
        
        if msg_consumed_count >= msg_count:
            break
                    
    consumer_timing = time.time() - consumer_start
    consumer.close()
    return consumer_timing

print 'Time used consuming messages: %f seconds' % python_kafka_consumer_performance()
