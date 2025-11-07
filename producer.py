# task2
from kafka import KafkaProducer
from configs import kafka_config
import json
import time
import random
import uuid

# Unique identifier
user_tag = "Serhii"
topic_name = f"{user_tag}_building_sensors"

# Each script run = new sensor with random ID
sensor_id = random.randint(1000, 9999)

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: str(v).encode("utf-8")
)

print(f"Sensor {sensor_id} started sending data to topic: {topic_name}")

# Send 20 messages with temperature and humidity
for i in range(20):
    try:
        message = {
            "sensor_id": sensor_id,
            "timestamp": time.time(),
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85)
        }

        producer.send(topic_name, key=str(uuid.uuid4()), value=message)
        producer.flush()

        print(f"[{i+1}] Data sent: {message}")
        time.sleep(2)  # pause between messages

    except Exception as e:
        print(f"Error while sending message: {e}")
        break

# Close producer
producer.close()
print("Sensor stopped.")