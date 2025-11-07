# task4
from kafka import KafkaConsumer
from configs import kafka_config
import json

# User tag
user_tag = "Serhii"

# Topics for alerts
topic_temp_alerts = f"{user_tag}_temperature_alerts"
topic_hum_alerts = f"{user_tag}_humidity_alerts"

# Initialize Kafka consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"{user_tag}_alerts_consumer"
)

# Subscribe to both topics
consumer.subscribe([topic_temp_alerts, topic_hum_alerts])

print(f"Listening for alerts on topics: {topic_temp_alerts}, {topic_hum_alerts}\n")

try:
    for record in consumer:
        alert_data = record.value
        key = record.key
        topic = record.topic

        print(f"[{topic}] Alert received:")
        print(f"  Key: {key}")
        print(f"  Data: {alert_data}")
        print("-" * 50)

except Exception as e:
    print(f"Error while consuming alerts: {e}")

finally:
    consumer.close()
    print("Consumer connection closed.")