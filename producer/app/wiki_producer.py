import requests
import sseclient
import json
from kafka import KafkaProducer

WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/page-create"
KAFKA_TOPIC = "input"
KAFKA_SERVER = "kafka-server:9092"

def main():
    session = requests.Session()
    client = sseclient.SSEClient(WIKI_STREAM_URL, session=session)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all'
    )

    try:
        for event in client:
            if event.event == "message":
                if not event.data.strip():
                    continue
                print(f"Received event: {event.data}")
                try:
                    data = json.loads(event.data)
                    future = producer.send(KAFKA_TOPIC, value=data)
                    future.get(timeout=10)
                    print(f"Sent to Kafka: {data.get('meta', {}).get('id', 'no_id')}")
                except Exception as e:
                    print(f"JSON parsing error: {e}")
    except Exception as stream_error:
        print(f"Error reading from stream: {stream_error}")

if __name__ == "__main__":
    main()