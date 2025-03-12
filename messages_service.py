from fastapi import FastAPI

app = FastAPI()

@app.get("/static_msg")
def static_msg():
    return "not implemented yet"