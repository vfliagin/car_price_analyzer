# -*- coding: utf-8 -*-
"""
Created on Sat Mar 23 18:57:30 2024

@author: CoffeeDrinker
"""
import csv
import json
from time import sleep
from confluent_kafka import Producer

class CarProducer():
    
    def __init__(self, bootstrap_servers, topic, file_path):
        self.conf = {'bootstrap.servers': bootstrap_servers}
        self.topic = topic
        self.file_path = file_path
        self.producer = Producer(self.conf)
        
    def simulate_data_collecting(self):
        """Симуляция сбора данных в реальном времени"""
        with open(self.file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    self.producer.produce(self.topic, key='1', value=json.dumps(row))
                    self.producer.flush()
                    print('produced:', row)
                except:
                    print('Kafka exception')
                sleep(2)
        
        print('All data used')
                
if __name__ == '__main__':
    
    bootstrap_servers = 'localhost:9095'
    topic = 'new_cars_topic'
    file_path = 'car_prices.csv'
    
    prod = CarProducer(bootstrap_servers, topic, file_path)
    prod.simulate_data_collecting()
    
