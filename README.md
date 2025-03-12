# SoftwareArchitecture

HW3

To install:

pip install fastapi

pip install uvicorn

pip install grpcio grpcio-tools

Also install hazelcast on python

pip install hazelcast-python-client


To setup:

uvicorn config_server:app --port 8007

uvicorn facade_service:app --port 8000

uvicorn messages_service:app --port 8002

python logging_service.py --port 8001

python logging_service.py --port 8003

python logging_service.py --port 8004


