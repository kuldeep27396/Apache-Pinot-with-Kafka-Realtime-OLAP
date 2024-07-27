import psutil
import json
from confluent_kafka import Producer
import time

# Kafka configuration
conf = {
    'bootstrap.servers': 'kafka:9092',  # Use the service name defined in docker-compose
    'client.id': 'system-info-producer'
}

# Initialize Kafka producer
producer = Producer(conf)

# Define a callback for delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to gather system information
def get_system_info():
    mem_info = psutil.virtual_memory()._asdict()
    info = {
        'cpu_percent': str(psutil.cpu_percent(interval=1)),
        'process_count': str(len(psutil.pids())),
        'memory_total': str(mem_info['total']),
        'memory_available': str(mem_info['available']),
        'memory_percent': str(mem_info['percent']),
        'timestamp': str(int(time.time() * 1000))  # Current time in milliseconds
    }
    return info

# Main function to produce messages to Kafka
def produce_system_info():
    while True:
        data = get_system_info()
        message = json.dumps(data)
        producer.produce('system-info-topic', message, callback=delivery_report)
        producer.poll(1)  # Wait up to 1 second for events to be delivered
        time.sleep(3)  # Sleep for 3 seconds before sending the next message

    # Ensure all messages are sent
    producer.flush()

if __name__ == "__main__":
    produce_system_info()
