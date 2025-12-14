from kafka import KafkaProducer
import json, time, random
from datetime import datetime
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def gen_transaction():
    """Generate a fake transaction event."""
    return {
        'transaction_id': fake.uuid4(),
        'timestamp': datetime.now().isoformat(),
        'user_id': f"USER_{random.randint(1, 500):05d}",
        'amount': round(random.uniform(5, 500), 2),
        'merchant': fake.company(),
        'category': random.choice(['Shopping', 'Food', 'Transport']),
    }

print("Sending transactions... (Ctrl+C to stop)")
count = 0

try:
    while True:
        producer.send('transactions', gen_transaction())
        count += 1
        if count % 50 == 0:
            print(f"Sent {count} transactions...")
        time.sleep(0.2)
except KeyboardInterrupt:
    producer.close()
    print(f"\nStopped. Total: {count}")
