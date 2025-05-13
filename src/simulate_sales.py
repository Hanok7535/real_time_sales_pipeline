#simulation-  infinite stream __ Ctrl+C to stop.


import time
import random
import json
from datetime import datetime, timezone

# Sample data options
products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse"]
regions = ["North", "South", "East", "West"]

def generate_sale():
    sale = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "region": random.choice(regions),
        "product": random.choice(products),
        "quantity": random.randint(1, 5),
        "price_per_unit": round(random.uniform(100.0, 1500.0), 2)
    }
    return sale

def stream_sales(delay=1):
    print("Streaming sales data...\n")
    while True:
        sale = generate_sale()
        print(json.dumps(sale))
        time.sleep(delay)

if __name__ == "__main__":
    stream_sales()