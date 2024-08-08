import json

from kafka import KafkaProducer

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send some messages
message = {
    "taskNo": "TASK202405053234357",
    "status": 1,
    "operator": "104074",
    "department": 9527,
    "active": True,
    "changeDt": "2024-08-08 18:06:01"
}
producer.send('work-order', message)
print(f'Sent: {message}')
message = {
    "taskNo": "TASK202405053234358",
    "status": 1,
    "operator": "106392",
    "department": 9527,
    "active": True,
    "changeDt": "2024-08-08 18:06:02"
}
producer.send('work-order', message)
print(f'Sent: {message}')
# Close the producer
producer.flush()
producer.close()
