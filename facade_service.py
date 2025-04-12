from fastapi import FastAPI
from pydantic import BaseModel
import grpc
import logging_pb2
import logging_pb2_grpc
import requests
import uuid
import time
import random
import json
from kafka import KafkaProducer

app = FastAPI()

CONFIG_SERVER_URL = "http://localhost:8007"

LOGGING_URL = requests.get(f"{CONFIG_SERVER_URL}/services_address/logging-service").json()["endpoints"]
MESSAGES_URL = requests.get(f"{CONFIG_SERVER_URL}/services_address/messages-service").json()["endpoints"]

print(f"Logging service URL: {LOGGING_URL}")
print(f"Messages service URL: {MESSAGES_URL}")
class Message(BaseModel):
    msg: str

KAFKA_SERVERS = ['localhost:29092', 'localhost:39092']
KAFKA_TOPIC = "messages"
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )
    print("Connected to Kafka")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    producer = None

def post_log_with_retry(msg_id, msg, retries=2, delay=2):
    for attempt in range(retries):
        response = None
        while response is None:
            try:
                with grpc.insecure_channel(random.choice(LOGGING_URL)) as channel:
                    stub = logging_pb2_grpc.LoggingServiceStub(channel)
                    response = stub.Log(logging_pb2.LogRequest(id=msg_id, msg=msg))
                    return response
            except grpc.RpcError as e:
                if attempt < retries - 1:
                    print(f"Error: {e}. Attempt {attempt}. Retrying in {delay} seconds...")
                    time.sleep(delay)

                else:
                    raise e

@app.post("/post_message")
def post_message(message: Message):
    msg = message.msg
    msg_id = str(uuid.uuid4())

    try:
        log_response = post_log_with_retry(msg_id, msg)
    except grpc.RpcError:
        return {"error": "Error contacting logging-service"}, 500
    if producer:
        try:
            message_data = {
                "id": msg_id,
                "msg": msg,
                "timestamp": time.time()
            }
            producer.send(KAFKA_TOPIC, value=message_data)
            producer.flush()
            print(f"Message sent to Kafka: {message_data}")
        except Exception as e:
            print(f"Error sending to Kafka: {e}")
            return {"status": log_response.status, "kafka_error": str(e)}
    else:
        return {"status": log_response.status, "kafka_warning": "Kafka producer not available"}

    return {"status": log_response.status, "kafka_status": "Message sent to queue"}


@app.get("/get_data")
def get_data():
    # Get logs from logging service
    retries = 0
    log_response = None
    while log_response is None and retries < 20:
        try:
            chosen = random.choice(LOGGING_URL)
            print(f"Chosen logging service: {chosen}")
            with grpc.insecure_channel(chosen) as channel:
                stub = logging_pb2_grpc.LoggingServiceStub(channel)
                log_response = stub.GetLogs(logging_pb2.Empty())
        except Exception as e:
            print(f"Error: {e}. Retrying...")
            retries += 1
    
    if log_response is None:
        return {"error": "Error contacting logging-service"}
    
    # Get messages from a randomly selected messages-service
    messages_service_url = random.choice(MESSAGES_URL)
    print(f"Chosen messages service: {messages_service_url}")
    
    try:
        messages_response = requests.get(f"{messages_service_url}/static_msg")
        if messages_response.status_code != 200:
            return {"error": f"Error retrieving data from messages-service: {messages_response.status_code}"}
        
        messages_data = messages_response.json()
    except Exception as e:
        return {"error": f"Error contacting messages-service: {e}"}
    
    # Convert log_response.logs to a serializable format
    logs = log_response.logs
    
    return {
        "logs": logs, 
        "messages": messages_data,
        "messages_service_instance": messages_service_url
    }

if __name__ == "__main__":
    import uvicorn
    port = 8000
    print(f"Starting facade service on port {port}")
    uvicorn.run(app,host='0.0.0.0', port=port)