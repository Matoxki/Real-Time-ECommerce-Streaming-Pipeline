import json
import os
import random
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from faker import Faker
from confluent_kafka import Producer

# Load environment variables from .env file
load_dotenv()

fake = Faker()

# Confluent Kafka Configuration
conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD'),
}

topic = os.getenv('KAFKA_TOPIC', 'raw_clicks')

# Creates a fixed pool of 50 active users.
# This ensures users generate repeat clicks over time, allowing your 
# dbt window functions (LAG) to correctly group them into sessions.
ACTIVE_USERS = [f"user_{i}" for i in range(100, 150)]

# Callback function to confirm message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Sent event to {msg.topic()} [Partition: {msg.partition()}]")

def generate_click_event():
    """Generates a realistic e-commerce clickstream event payload."""
    event_types = ['page_view', 'add_to_cart', 'remove_from_cart', 'checkout_start', 'purchase_complete']
    devices = ['mobile', 'desktop', 'tablet']
    
    return {
        'event_id': fake.uuid4(),
        # Pick from our fixed pool instead of generating a brand new user every millisecond
        'user_id': random.choice(ACTIVE_USERS), 
        'event_type': random.choices(event_types, weights=[50, 25, 10, 10, 5])[0],
        'product_id': f"prod_{random.randint(100, 500)}",
        'price': round(random.uniform(5.0, 250.0), 2),
        'device': random.choices(devices, weights=[60, 30, 10])[0],
        'ip_address': fake.ipv4(),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

def main():
    producer = Producer(conf)
    print(f"🚀 Streaming real-time e-commerce events to topic '{topic}'... Press Ctrl+C to stop.")

    try:
        while True:
            event = generate_click_event()
            # Serialize JSON payload
            payload = json.dumps(event).encode('utf-8')
            
            # Produce message asynchronously
            producer.produce(
                topic=topic,
                key=event['user_id'],  # Guarantees user events land in the same partition
                value=payload,
                callback=delivery_report
            )
            
            # Poll events to trigger delivery callbacks
            producer.poll(0)
            
            # Simulate real-time stream interval (0.2s to 1.5s between clicks)
            time.sleep(random.uniform(0.2, 1.5))

    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    finally:
        # Flush remaining messages before exiting
        print("Flushing outstanding messages...")
        producer.flush()

if __name__ == '__main__':
    main()