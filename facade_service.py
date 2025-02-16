from fastapi import FastAPI
from pydantic import BaseModel
import grpc
import logging_pb2
import logging_pb2_grpc
import requests
import uuid
import time 
app = FastAPI()

LOGGING_URL = "localhost:8001"
MESSAGES_URL = "http://localhost:8002"

class Message(BaseModel):
    msg: str

def post_log_with_retry(msg_id, msg, retries=5, delay=2):
    for attempt in range(retries):
        try:
            with grpc.insecure_channel(LOGGING_URL) as channel:
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
    try:
        with grpc.insecure_channel(LOGGING_URL) as channel:
            stub = logging_pb2_grpc.LoggingServiceStub(channel)
            log_response = stub.GetLogs(logging_pb2.Empty())
        messages_response = requests.get(f"{MESSAGES_URL}/static_msg")
    except (grpc.RpcError, requests.exceptions.RequestException):
        return {"error": "Error contacting services"}, 500
    if messages_response.status_code != 200:
        return {"error": "Error retrieving data"}, 500
    
    # Convert log_response.logs to a serializable format
    logs = log_response.logs
    
    return {"logs": logs, "messages": messages_response.text}