# SoftwareArchitecture

HW4

Слід спочатку запустити config_server.py та докер(кафка)

python config_server.py

docker compose up

Також запустити скрипт topic.sh

./topic.sh

Після цього facade_server.py

python facade_server.py

python logging_service.py --port 8001

python logging_service.py --port 8002

python logging_service.py --port 8003

python message_service.py 8004

python message_service.py 8005

