from kafka import KafkaConsumer
import json

def consume_sales():
    consumer = KafkaConsumer(
        'sales',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='sales-group'
    )

    print("Consumer started. Waiting for messages...")

    try:
        for message in consumer:
            print(f"Received: {message.value}")
    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        consumer.close()

if __name__ == '__main__':
    consume_sales()

