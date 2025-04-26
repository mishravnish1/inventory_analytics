#!/usr/bin/env python
# coding: utf-8

# In[2]:


import random
from datetime import datetime, timedelta
import json 
from utils import get_logger, createProducer


# In[19]:


def timestamp():
    return (datetime.now() - timedelta(random.randint(0, 30))).strftime("%Y-%m-%dT%H:%M:%S") 
    # time zone of databrick is 00:00, ours is +05:30


# In[20]:


def message():
    value = single_msg = {"timestamp": timestamp(),
                          "user_id": random.randint(1000, 9999),
                          "product_id": f"P{random.randint(1, 1000)}",
                          "pin_code": random.randint(400001, 400600),
                          }
    return json.dumps(value)


# In[21]:


def produce():
    logger = get_logger(__name__)
    producer = createProducer(name="product_click")

    j = 0
    while True:
        for i in range(1000):
            producer.produce("product_click", key=f'test_{i+j+100000}', value=message())
        producer.poll()
        j += 1
    producer.flush()

if __name__ == "__main__":
    produce()