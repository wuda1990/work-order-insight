################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import calendar
import random
import time
from json import dumps
from random import randint
from time import sleep

from kafka import KafkaProducer
from kafka import errors


def write_data(producer):
    data_cnt = 20000
    task_no = calendar.timegm(time.gmtime())
    operators = ["104074", "10369", "12345"]
    departments = [9527, 760, 888]
    topic = "work-order"

    for i in range(data_cnt):
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        task_no += 1
        active = True if random.random() < 0.9 else False
        operator_index = randint(0, 2)
        department_index = randint(0, 2)
        status = randint(1, 5)
        cur_data = {
            "taskNo": f"TASK{task_no}",
            "status": status,
            "operator": operators[operator_index],
            "department": departments[department_index],
            "active": active,
            "changeDt": ts
        }
        producer.send(topic, value=cur_data)
        print(f'Sent: {cur_data}')
        sleep(0.5)


def create_producer():
    print("Connecting to Kafka brokers")
    for i in range(0, 6):
        try:
            producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                     value_serializer=lambda x: dumps(x).encode('utf-8'))
            print("Connected to Kafka")
            return producer
        except errors.NoBrokersAvailable:
            print("Waiting for brokers to become available")
            sleep(10)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")


if __name__ == '__main__':
    producer = create_producer()
    write_data(producer)
