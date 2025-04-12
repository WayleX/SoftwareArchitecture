from fastapi import FastAPI
import json 

app = FastAPI()

PATH = "config.json"
services = json.load(open(PATH))

@app.get("/services_address/{service_name}")
def get_service(service_name: str):
    if service_name in services:
        return {"name": service_name, "endpoints": services[service_name]}
    return {"error": f"Service '{service_name}' not found"}, 404

if __name__ == "__main__":
    import uvicorn
    port = 8007
    print(f"Starting config server on port {port}")
    uvicorn.run(app,host= "0.0.0.0", port=port)
