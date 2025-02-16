from fastapi import FastAPI
from pydantic import BaseModel
import requests
import uuid
import time 
app = FastAPI()

LOGGING_URL = "http://localhost:8001"
MESSAGES_URL = "http://localhost:8002"

class Message(BaseModel):
    msg: str

def post_log_with_retry(msg_id, msg, retries=3, delay=2):
    for attempt in range(retries):
        try:
            response = requests.post(f"{LOGGING_URL}/log", json={"id": msg_id, "msg": msg})
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise e

@app.post("/post_message")
def post_message(message: Message):
    msg = message.msg
    msg_id = str(uuid.uuid4())
    
    try:
        post_log_with_retry(msg_id, msg)
    except requests.exceptions.RequestException:
        return {"error": "Error contacting logging-service"}, 500
    return {"status": f"Message received with id: {msg_id}"}


@app.get("/get_data")
def get_data():
    try:
        log_response = requests.get(f"{LOGGING_URL}/logs")
        messages_response = requests.get(f"{MESSAGES_URL}/static_msg")
    except requests.exceptions.RequestException:
        return {"error": "Error contacting services"}, 500
    if log_response.status_code != 200 or messages_response.status_code != 200:
        return {"error": "Error retrieving data"}, 500
    return {"logs": log_response.text, "messages": messages_response.text}



