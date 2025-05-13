#simulation- finitie defined stream __ simulation stops after "max_sales".

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

def stream_sales(max_sales=10):  #("argument to stop simulation")
    for _ in range(max_sales):
        sale = generate_sale()
        print(json.dumps(sale))
        time.sleep(1)

if __name__ == "__main__":
    stream_sales(max_sales=10)  # Change this number as needed