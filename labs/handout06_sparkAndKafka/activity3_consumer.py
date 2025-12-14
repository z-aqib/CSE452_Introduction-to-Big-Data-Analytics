from kafka import KafkaConsumer
import json, signal, sys

# Create Kafka consumer listening to 'sensor-data'
consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',      # start from earliest messages if no offset
    group_id='anomaly-detector',       # consumer group name
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def signal_handler(sig, frame):
    """Graceful shutdown when you press Ctrl+C."""
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

print("=" * 70)
print("Kafka Consumer - Real-Time Anomaly Detection")
print("=" * 70)
print("Listening for sensor data... (Press Ctrl+C to stop)\n")

count = 0
anomaly_count = 0

for msg in consumer:
    data = msg.value      # this is the dict we sent from the producer
    count += 1

    # -------------------------
    # Anomaly Detection Logic
    # -------------------------
    is_anomaly = False
    reasons = []

    # Condition 1: High temperature
    if data['temperature'] > 32:
        is_anomaly = True
        reasons.append(f"High temp: {data['temperature']}°C")

    # Condition 2: Critical status
    if data['status'] == 'critical':
        is_anomaly = True
        reasons.append("Critical status")

    # Condition 3: (EXTRA) very high humidity
    if data['humidity'] > 75:  # //extra – new anomaly rule
        is_anomaly = True      # //extra
        reasons.append(f"High humidity: {data['humidity']}%")  # //extra

    # If any condition triggered, print anomaly
    if is_anomaly:
        anomaly_count += 1
        print(f"\nANOMALY DETECTED (#{anomaly_count})")
        print(f"  Sensor:  {data['sensor_id']}")
        print(f"  Time:    {data['timestamp']}")
        print(f"  Reasons: {', '.join(reasons)}")
        print(f"  Full Data: {data}")

    # Progress log every 50 messages
    if count % 50 == 0:
        print(f"\n--- Processed: {count} | Anomalies: {anomaly_count} ---")
