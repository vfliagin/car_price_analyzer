from time import sleep, time
import streamlit as st
import altair as alt
import plotly.express as px
import pandas as pd
import json
from confluent_kafka import Consumer

st.set_page_config(
    page_title="Real-Time Data Dashboard",
    layout="wide",
)

alt.themes.enable("dark")

st.header('Car prices in the USA')

if "price" not in st.session_state:
    st.session_state["price"] = []
    
if "time" not in st.session_state:
    st.session_state["time"] = []
    
if "brand" not in st.session_state:
    st.session_state["brand"] = []

if "location" not in st.session_state:
    st.session_state["location"] = []

bootstrap_servers = 'localhost:9095'
topic = 'cars_analyzed_topic'

conf = {'bootstrap.servers': bootstrap_servers, 'group.id': 'visual_consumers'}

consumer = Consumer(conf)
consumer.subscribe([topic])

chart_holder = st.empty()
    
while True:
    msg = consumer.poll(1000)

    if msg is not None:
        print('recived new msg')
        car_data = json.loads(msg.value().decode('utf-8'))
        print(car_data)
        st.session_state["price"].append(car_data['price'])
        st.session_state["brand"].append(car_data['make'])
        st.session_state["location"].append(car_data['state'])
        st.session_state["time"].append(time())
    
    price_data = {'price': st.session_state["price"], 'time': st.session_state["time"]}
    price_df = pd.DataFrame(price_data)
    
    brand_data = {'brand': st.session_state["brand"]}
    brand_df = pd.DataFrame(brand_data)
    
    with chart_holder.container():
        
        price_col, made_col = st.columns(2)
        
        with price_col:
            st.markdown("### Prices")
            fig = px.line(price_df, x='time', y='price')
            st.write(fig)
            
        with made_col:
            st.markdown("### Car brands")
            fig2 = px.histogram(data_frame=brand_data, x="brand")
            st.write(fig2)
    
        usa_col, empty_col = st.columns(2)
    
        with usa_col:
    
            max_color = 10        
    
            location = st.session_state["location"]
            loc_data = {'location': location}
            loc_df = pd.DataFrame(loc_data)
    
            states = list(loc_df.location.unique())
            counts = list(loc_df.groupby(location).count().location)
    
            states = [state.upper() for state in states]
       
            if max(counts) > max_color:
                max_color = max(counts)
    
            st.markdown("### Offer location")
            fig3 = px.choropleth(locations=states, color = counts,
                               locationmode="USA-states",
                               color_continuous_scale = 'greens',
                               range_color=(0, max_color),
                               scope="usa")
    
            st.write(fig3)
         
    sleep(1)

    
    
    

    
