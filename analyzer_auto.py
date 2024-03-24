# -*- coding: utf-8 -*-
"""
Created on Sun Mar 24 02:55:53 2024

@author: CoffeeDrinker
"""

import json
import pickle
import pandas as pd
from sklearn.preprocessing import OrdinalEncoder
from xgboost import XGBRegressor
from confluent_kafka import Producer, Consumer

class CarAnalyzer():
    
    def __init__(self, bootstrap_servers, consume_topic, produce_topic, group_id):
        self.conf_cons = {'bootstrap.servers': bootstrap_servers, 'group.id': group_id}
        self.conf_prod = {'bootstrap.servers': bootstrap_servers}
        self.consume_topic = consume_topic
        self.produce_topic = produce_topic
        self.consumer = Consumer(self.conf_cons)
        self.producer = Producer(self.conf_prod)
        self.consumer.subscribe([consume_topic])
           
        with open('model.pkl', 'rb') as f:
            self.model = pickle.load(f)
            
        with open('encoder.pkl', 'rb') as f:
            self.encoder = pickle.load(f)
        
            
        self.cat_rows = ['make', 'model', 'trim', 'body',
                         'transmission', 'state', 'color', 'interior']
    
    def analyze_row(self, car_dict):
        
        print('analyzing:', car_dict)
        
        for col in car_dict:
            car_dict[col] = [car_dict[col]]
        
        df = pd.DataFrame.from_dict(car_dict)
        
        for col in df.columns:
            if col not in self.cat_rows:
                df[col] = df[col].astype('int')
        
        try:
            df_trans = self.encoder.transform(df)
        except:
            print('Unknown categories')
            return None
        
        pred = self.model.predict(df_trans)
        
        car_dict = df.to_dict()
        
        print(car_dict)
        
        for col in car_dict:
            car_dict[col] = car_dict[col][0]
        
        car_dict['price'] = int(pred[0])
        
        print(car_dict)
        
        return car_dict
    
    def analyze_data(self):
        """Предсказание стоимости машин"""
        
        while True:
            msg = self.consumer.poll(1000)
            if msg:
                car_data = json.loads(msg.value().decode('utf-8'))
                analyzed_data = self.analyze_row(car_data)
                if analyzed_data is not None:
                    self.producer.produce(self.produce_topic,
                                              key='1', value=json.dumps(analyzed_data))
                    self.producer.flush()
                    print('analyzed:', analyzed_data)
        
if __name__ == '__main__':
    
    bootstrap_servers = 'localhost:9095'
    consume_topic = 'cars_processed_topic'
    produce_topic = 'cars_analyzed_topic'
    group_id = 'processed_consumers'
    
    an = CarAnalyzer(bootstrap_servers, consume_topic, produce_topic, group_id)
    an.analyze_data()
    
    
    
    