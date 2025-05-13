from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PRODUCTS = ["Laptop", "Phone", "Tablet", "Monitor", "Headphones"]

def generate_sale():
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "product": random.choice(PRODUCTS),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(100, 1000), 2)
    }

def stream_sales():
    while True:
        sale = generate_sale()
        producer.send("sales_topic", sale)
        print("Sent:", sale)
        time.sleep(1)

if __name__ == "__main__":
    stream_sales()
