import pandas as pd
import json, time
from kafka import KafkaProducer

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv("dummy_data/data.csv")

for _, row in df.iterrows():
    message = row.to_dict()
    producer.send("sensor-stream", message)
    print("Sent:", message)
    time.sleep(1)   
