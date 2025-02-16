# SoftwareArchitecture

If not installed:

```shell
pip install fastapi
pip install uvicorn
pip install grpcio grpcio-tools
```

To launch 

Facade service
```shell
uvicorn facade_service:app --port 8000
```
Logging service
```shell
python logging_service.py 
```
Messages service
```shell
uvicorn messages_service:app --port 8002
```