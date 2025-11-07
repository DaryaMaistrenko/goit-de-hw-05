# task1
from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Initialize Kafka Admin client
admin = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Unique identifier to avoid topic name collisions
user_tag = "Serhii"

# Define topics for IoT use case
topics = [
    f"{user_tag}_building_sensors",
    f"{user_tag}_temperature_alerts",
    f"{user_tag}_humidity_alerts"
]

# Topic configuration
partitions = 2
replicas = 1

# Prepare list of topics to create
topics_definitions = [
    NewTopic(name=topic, num_partitions=partitions, replication_factor=replicas)
    for topic in topics
]

# Create topics on Kafka broker
try:
    admin.create_topics(new_topics=topics_definitions, validate_only=False)
    print("Topics were successfully created:")
    for t in topics:
        print(f"  - {t}")
except Exception as err:
    print(f"Failed to create topics: {err}")

# Verify created topics
print("\nAvailable topics containing user tag:")
for t in admin.list_topics():
    if user_tag in t:
        print(f"  - {t}")

# Close connection
admin.close()