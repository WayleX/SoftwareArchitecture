# SoftwareArchitecture

HW4

Слід спочатку запустити consul agent -dev -client=0.0.0.0 та докер(кафка)

consul agent -dev -client=0.0.0.0

docker compose up

Також запустити скрипт topic.sh

./topic.sh

Після цього facade_server.py

python facade_server.py

python logging_service.py --port 8001

python logging_service.py --port 8002

python logging_service.py --port 8003

python messages_service.py 8004

python messages_service.py 8005

