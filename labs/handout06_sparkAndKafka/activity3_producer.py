from kafka import KafkaProducer
import json, time, random
from datetime import datetime
from faker import Faker

fake = Faker()

# Kafka producer: sends Python dict as JSON to localhost:9092
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def gen_sensor():
    """
    Generate one fake sensor reading:
    - sensor_id: SENSOR_001 ... SENSOR_020
    - timestamp: current time in ISO format
    - temperature: 15–35 °C
    - humidity: 30–80 %
    - pressure: 980–1050 hPa
    - status: mostly 'normal', sometimes 'warning' or 'critical'
    """
    return {
        'sensor_id': f"SENSOR_{random.randint(1, 20):03d}",
        'timestamp': datetime.now().isoformat(),
        'temperature': round(random.uniform(15, 35), 2),
        'humidity': round(random.uniform(30, 80), 2),
        'pressure': round(random.uniform(980, 1050), 2),
        'status': random.choice(['normal', 'normal', 'normal', 'warning', 'critical'])
    }

print("=" * 70)
print("Kafka Producer - Sending Sensor Data")
print("=" * 70)

# Send 500 sensor messages
for i in range(500):
    msg = gen_sensor()
    producer.send('sensor-data', msg)

    if (i + 1) % 100 == 0:
        print(f"Sent {i+1} messages...")

    time.sleep(0.05)

producer.flush()
producer.close()
print("\nComplete! Sent 500 sensor readings")
