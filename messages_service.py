from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json
import os
import time
import sys
import uuid
import consul

app = FastAPI()

messages = []

c = consul.Consul()

service_name = "messages-service"




SERVICE_ID = os.environ.get("SERVICE_ID", str(uuid.uuid4())[:8])


index, data = c.kv.get('kafka_servers')
kafka_servers = data['Value'].decode()[1:-1] if data and data['Value'] else None
index, data = c.kv.get('kafka_topic')
kafka_topic = data['Value'].decode()[1:-1] if data and data['Value'] else None
KAFKA_SERVERS = kafka_servers.split(",")
KAFKA_TOPIC = kafka_topic


def consume_messages():
    """Consume messages from Kafka in a separate thread."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f'messages-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        print(f"[{SERVICE_ID}] Connected to Kafka, consuming messages...")
        
        for message in consumer:
            msg_value = message.value
            print(f"[{SERVICE_ID}] Received message: {msg_value}")
            messages.append(msg_value)
    except Exception as e:
        print(f"[{SERVICE_ID}] Error consuming Kafka messages: {e}")
        time.sleep(5)  # Wait and retry
        consume_messages()

# Start the Kafka consumer in a separate thread
consumer_thread = threading.Thread(target=consume_messages, daemon=True)
consumer_thread.start()

@app.get("/static_msg")
def static_msg():
    return {"instance_id": SERVICE_ID, "messages": messages}

@app.get("/health")
def health():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8002
    print(f"Starting messages service with ID {SERVICE_ID} on port {port}")
    c.agent.service.register(
        name=service_name,
        service_id=SERVICE_ID,
        port=port,
        check=consul.Check.http(
            url=f"http://localhost:{port}/health",
            interval="10s"
        )
    )
    uvicorn.run(app, host="0.0.0.0", port=port)