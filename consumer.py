# task3
from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json
import time

# User tag
user_tag = "Serhii"

# Topics
topic_sensors = f"{user_tag}_building_sensors"
topic_temp_alerts = f"{user_tag}_temperature_alerts"
topic_hum_alerts = f"{user_tag}_humidity_alerts"

# Consumer (read sensors)
consumer = KafkaConsumer(
    topic_sensors,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='Serhii_consumer_group'
)

# Producer (send alerts)
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: str(v).encode("utf-8")
)

print(f"Subscribed to topic '{topic_sensors}'")
print("Listening for messages...\n")

try:
    for msg in consumer:
        data = msg.value
        sensor_id = data.get("sensor_id")
        temperature = data.get("temperature")
        humidity = data.get("humidity")
        timestamp = data.get("timestamp")

        print(f"Received: {data}")

        # Check temperature
        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": time.time(),
                "temperature": temperature,
                "message": f"⚠️ High temperature detected: {temperature}°C"
            }
            producer.send(topic_temp_alerts, key=sensor_id, value=alert)
            producer.flush()
            print(f"Sent temperature alert: {alert}")

        # Check humidity
        if humidity > 80 or humidity < 20:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": time.time(),
                "humidity": humidity,
                "message": f"⚠️ Abnormal humidity detected: {humidity}%"
            }
            producer.send(topic_hum_alerts, key=sensor_id, value=alert)
            producer.flush()
            print(f"Sent humidity alert: {alert}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    consumer.close()
    producer.close()
    print("Consumer & Producer closed.")