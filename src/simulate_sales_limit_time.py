#simulation- finitie defined stream __ simulation stops after "duration_seconds".
import time
import json
import random
from datetime import datetime, timezone

def generate_sale():
    return {
        "product_id": random.randint(1000, 9999),
        "amount": round(random.uniform(10.0, 500.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def stream_sales(duration_seconds=10):
    start_time = time.time()
    while time.time() - start_time < duration_seconds:
        sale = generate_sale()
        print(json.dumps(sale))
        time.sleep(1)

if __name__ == "__main__":
    stream_sales(duration_seconds=10)  # Change this duration as needed