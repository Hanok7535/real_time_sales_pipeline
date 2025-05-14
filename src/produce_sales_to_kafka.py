from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timezone

products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse"]
regions = ["North", "South", "East", "West"]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sale():
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "region": random.choice(regions),
        "product": random.choice(products),
        "quantity": random.randint(1, 5),
        "price_per_unit": round(random.uniform(100.0, 1500.0), 2)
    }

def stream_sales_to_kafka(topic='sales', delay=1):
    print("Producing messages to Kafka...\n")
    while True:
        sale = generate_sale()
        producer.send(topic, sale)
        print(f"Sent: {sale}")
        time.sleep(delay)

if __name__ == "__main__":
    stream_sales_to_kafka()
