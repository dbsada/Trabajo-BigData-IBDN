import os
from utils.shell import sh


KAFKA_TOPICS = [
    os.getenv('KAFKA_TOPIC', 'flight-delay-ml-request'),
    os.getenv('KAFKA_RESPONSE_TOPIC', 'flight-delay-ml-response'),
    os.getenv('KAFKA_STATUS_TOPIC', 'flight-delay-ml-status'),
]


@sh
def create_topic(topic_name):
    kafka_local = os.getenv('KAFKA_LOCAL_BOOTSTRAP_SERVERS', 'localhost:9092')
    container = os.getenv('KAFKA_CONTAINER', 'kafka')
    return (
        f"docker exec {container} /opt/kafka/bin/kafka-topics.sh "
        f"--create --bootstrap-server {kafka_local} "
        f"--topic {topic_name} --partitions 1 --replication-factor 1 --if-not-exists"
    )


def create_all_topics():
    results = []
    for topic in KAFKA_TOPICS:
        r = create_topic(topic)
        results.append(r)
    return results
