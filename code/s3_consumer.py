import json
import boto3
from kafka import KafkaConsumer

# CONFIG
BUCKET_NAME = "zomato-data-platform"
TOPIC_NAME = "zomato-orders"

s3 = boto3.client("s3")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', # Start from the beginning of the topic
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Consumer started. Waiting for messages in '{TOPIC_NAME}'...")

for message in consumer:
    order_data = message.value
    # We use the order_id as the filename in S3
    file_name = f"raw/orders/order_{order_data['order_id']}.json"
    
    # Upload to S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(order_data)
    )
    print(f"Copied from Kafka to S3: {file_name}")