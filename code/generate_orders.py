import json
import uuid
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# CONFIG
TOPIC_NAME = "zomato-orders"
MAX_FILES = 50 

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_order():
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "restaurant_id": random.randint(1, 100),
        "amount": round(random.uniform(5, 100), 2),
        "status": random.choice(["PLACED", "PREPARING", "DELIVERED", "CANCELLED"]),
        "order_time": datetime.now(timezone.utc).isoformat()
    }

print(f"Streaming to Kafka topic: {TOPIC_NAME}...")

try:
    for i in range(MAX_FILES):
        order = generate_order()
        
        # Send to Kafka
        producer.send(TOPIC_NAME, value=order)
        
        print(f"[{i+1}/{MAX_FILES}] Sent to Kafka: {order['order_id']}")
        time.sleep(random.randint(1, 3)) # Simulating real-time traffic

    producer.flush() # Ensure all messages are sent
    print("\nBatch complete.")

except KeyboardInterrupt:
    print("\nStopped by user.")