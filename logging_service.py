from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()
messages_store = {}

class LogInfo(BaseModel):
    id: str
    msg: str

@app.post("/log")
def log(payload: LogInfo):
    msg_id = payload.id
    msg = payload.msg
    if msg_id in messages_store:
        return {"status": "Duplicate message"}
    messages_store[msg_id] = msg
    print(f"Logged: {msg_id} -> {msg}")
    return {"status": "Logged"}


@app.get("/logs")
def get_logs():
    return "\n".join(messages_store.values())