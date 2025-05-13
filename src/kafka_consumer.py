from kafka import KafkaConsumer
import json

def consume_sales():
    consumer = KafkaConsumer(
        'sales_topic',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='sales-consumer-group'
    )

    print("Consumer started. Waiting for messages...")
    for message in consumer:
        print(f"Received: {message.value}")

if __name__ == "__main__":
    consume_sales()
