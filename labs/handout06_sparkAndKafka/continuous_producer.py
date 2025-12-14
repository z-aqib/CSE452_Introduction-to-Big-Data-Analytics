from kafka import KafkaProducer
import json, time, random
from datetime import datetime

# Kafka producer sending continuous sensor data
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def gen_sensor():
    """Generate one random sensor reading."""
    return {
        'sensor_id': f"SENSOR_{random.randint(1, 20):03d}",
        'timestamp': datetime.now().isoformat(),
        'temperature': round(random.uniform(15, 35), 2),
        'humidity': round(random.uniform(30, 80), 2),
        'pressure': round(random.uniform(980, 1050), 2),
        'status': random.choice(['normal', 'normal', 'warning', 'critical'])
    }

print("Sending continuous sensor data... (Ctrl+C to stop)")
count = 0

try:
    while True:
        producer.send('sensor-data', gen_sensor())
        count += 1
        if count % 100 == 0:
            print(f"Sent {count} messages...")
        time.sleep(0.1)
except KeyboardInterrupt:
    producer.close()
    print(f"\nStopped. Total sent: {count}")
