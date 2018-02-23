#!/usr/bin/python

from kafka import KafkaProducer
import sys
from time import sleep
from urllib2 import urlopen
from re import search

def main():
    #novel_filename = sys.argv[1]
    novel_url = sys.argv[1]

    #novel_name = get_novel_name(novel_filename)

    #novel_content = get_content(novel_filename)
    novel_content = get_content(novel_url)
    novel_name = get_novel_name(novel_content)

    handle_content(novel_content, novel_name)

'''
def get_content(novel_filename):
    with open('%s' % novel_filename, 'r') as f:
        content = f.read()

    content = content.replace('\r', ' ')
    content = content.replace('K.', 'K')
    content_sentences = [s.strip() for s in content.replace('\n', '').split('.')]
    return content_sentences
'''

def get_content(novel_url):
    content = urlopen(novel_url).read()

    content = content.replace('\r', ' ')
    content = content.replace('K.', 'K')
    content_sentences = [s.strip() for s in content.replace('\n', '').split('.')]
    return content_sentences



def handle_content(novel_content, novel_name):
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    i = 0
    print len(novel_content)
    for sentence in novel_content:
        if not sentence:
            continue

        i = i + 1

        sentence_length_chars = len(sentence)
        sentence_length_words = len(sentence.split())
        #print '%s | %i | %i' % (sentence, sentence_length_chars, sentence_length_words)
        producer.send('novel.%s' % novel_name, sentence)

        if sentence_length_chars > 100:
            producer.send('FM', '%s: too many characters (%i) in sentence number %i' % (novel_name, sentence_length_chars, i))

        producer.send('PM.chars_count', '%s: %i chars' % (novel_name, sentence_length_chars))
        producer.send('PM.words_count', '%s: %i words' % (novel_name, sentence_length_words))

        sleep(2)

'''
def get_novel_name(novel_filename):
    return novel_filename.split('.')[0]
'''

def get_novel_name(novel_content):
    m = search('(?<=Title: )[\w\s]+ ', '\n'.join(novel_content))
    novel_name = m.group(0)
    return novel_name.replace(' ', '')


main()
