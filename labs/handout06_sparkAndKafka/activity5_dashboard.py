from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
import json, threading, time

# Flask app and SocketIO setup
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Shared metrics dictionary (global state)
metrics = {
    'sensor_count': 0,
    'txn_count': 0,
    'current_temp': 0.0,
    'total_revenue': 0.0,
    'status': 'Running'
}

class KafkaThread(threading.Thread):
    """
    Background thread that consumes from a Kafka topic
    and updates the global 'metrics' dict. Then emits
    updates to all connected SocketIO clients.
    """
    def __init__(self, topic):
        super().__init__(daemon=True)
        self.topic = topic

    def run(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            group_id=f'{self.topic}_dash',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        for msg in consumer:
            data = msg.value

            # Update metrics depending on topic
            if self.topic == 'sensor-data':
                metrics['sensor_count'] += 1
                metrics['current_temp'] = data.get('temperature', 0.0)

            elif self.topic == 'transactions':
                metrics['txn_count'] += 1
                metrics['total_revenue'] += data.get('amount', 0.0)

            # Push update to all connected dashboards
            socketio.emit('update', metrics, namespace='/live')


@app.route('/')
def index():
    """Serve the dashboard HTML."""
    return render_template('dashboard.html')


@socketio.on('connect', namespace='/live')
def handle_connect():
    """
    When a new browser connects to /live namespace,
    immediately send it the current metrics snapshot.
    """
    emit('update', metrics)


if __name__ == '__main__':
    # Start Kafka consumer threads
    KafkaThread('sensor-data').start()
    KafkaThread('transactions').start()

    print("=" * 70)
    print("Real-Time Dashboard Server")
    print("=" * 70)
    print("Dashboard URL: http://localhost:5000")
    print("Press Ctrl+C to stop")
    print("=" * 70)

    socketio.run(app, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)
