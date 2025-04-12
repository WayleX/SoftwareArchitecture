from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json
import os
import time
import sys
import uuid

app = FastAPI()

messages = []

SERVICE_ID = os.environ.get("SERVICE_ID", str(uuid.uuid4())[:8])
KAFKA_SERVERS = ['localhost:9092', 'localhost:39092', 'localhost:49092']
KAFKA_TOPIC = "messages"

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


if __name__ == "__main__":
    import uvicorn
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8002
    print(f"Starting messages service with ID {SERVICE_ID} on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)