from fastapi import FastAPI
from pydantic import BaseModel
import grpc
import logging_pb2
import logging_pb2_grpc
import requests
import uuid
import time
import random
app = FastAPI()

CONFIG_SERVER_URL = "http://localhost:8007"

LOGGING_URL = requests.get(f"{CONFIG_SERVER_URL}/services_address/logging-service").json()["endpoints"]
MESSAGES_URL = requests.get(f"{CONFIG_SERVER_URL}/services_address/messages-service").json()["endpoints"][0]

print(f"Logging service URL: {LOGGING_URL}")
print(f"Messages service URL: {MESSAGES_URL}")
class Message(BaseModel):
    msg: str

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
        response = post_log_with_retry(msg_id, msg)
        return {"status": response.status}
    except grpc.RpcError:
        return {"error": "Error contacting logging-service"}, 500


@app.get("/get_data")
def get_data():
    retries = 0
    log_response = None
    messages_response = None
    while log_response is None:
        try:
            chosen = random.choice(LOGGING_URL)
            print(f"Chosen: {chosen}")
            with grpc.insecure_channel(chosen) as channel:
                stub = logging_pb2_grpc.LoggingServiceStub(channel)
                log_response = stub.GetLogs(logging_pb2.Empty())
        except:
            if retries < 20:
                print(f"Error:. Retrying...")
                retries += 1
            else:
                return {"error": "Error contacting logging-service"}, 500
    messages_response = requests.get(f"{MESSAGES_URL}/static_msg")

    if messages_response.status_code != 200:
        return {"error": "Error retrieving data"}, 500
    
    # Convert log_response.logs to a serializable format
    logs = log_response.logs
    
    return {"logs": logs, "messages": messages_response.text}