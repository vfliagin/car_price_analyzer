# -*- coding: utf-8 -*-
"""
Created on Sat Mar 23 19:42:41 2024

@author: CoffeeDrinker
"""

import json
import pandas as pd
from time import sleep
from confluent_kafka import Producer, Consumer

class CarProcessor():
    
    def __init__(self, bootstrap_servers, consume_topic, produce_topic, group_id):
        self.conf_cons = {'bootstrap.servers': bootstrap_servers, 'group.id': group_id}
        self.conf_prod = {'bootstrap.servers': bootstrap_servers}
        
        self.consume_topic = consume_topic
        self.produce_topic = produce_topic
        self.consumer = Consumer(self.conf_cons)
        self.producer = Producer(self.conf_prod)
        self.consumer.subscribe([consume_topic])
    
        #Объединение разных написаний
        self.make_transform = {'landrover': 'land rover', 'mercedes-b': 'mercedes-benz',
                               'mercedes': 'mercedes-benz', 'vw': 'volkswagen'}
        
    
    def process_row(self, car_dict):
        """Предобработка одной записи"""
        
        #Удаление лишних столбцов
        del car_dict['vin']
        del car_dict['seller']
        del car_dict['saledate']
        del car_dict['sellingprice']
    
        car_dict['make'] = car_dict['make'].lower()
        if car_dict['make'] in self.make_transform:
            car_dict['make'] = self.make_transform[car_dict['make']]
        
        car_dict['body'] = car_dict['body'].lower()
        
        return car_dict
    
    def process_data(self):
        """Предобработка данных для анализа"""
        
        while True:
            msg = self.consumer.poll(1000)
            if msg is not None:
                print(msg.value())
                crude_data = json.loads(msg.value().decode('utf-8'))
                clean_data = self.process_row(crude_data)
                self.producer.produce(self.produce_topic,
                                              key='1', value=json.dumps(clean_data))
                self.producer.flush()
                print('processed:', clean_data)

if __name__ == '__main__':
    
    bootstrap_servers = 'localhost:9095'
    consume_topic = 'new_cars_topic'
    produce_topic = 'cars_processed_topic'
    group_id = 'crude_consumers'
    
    proc = CarProcessor(bootstrap_servers, consume_topic, produce_topic, group_id)
    proc.process_data()
        
