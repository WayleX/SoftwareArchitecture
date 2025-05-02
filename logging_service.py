from concurrent import futures
import grpc
import logging_pb2
import logging_pb2_grpc
import hazelcast
import sys
import argparse
import subprocess
import atexit
import logging
import consul
import random

messages_store = {}






class LoggingService(logging_pb2_grpc.LoggingServiceServicer):
    def __init__(self,hz_client):
        self.hz_client = hz_client
        self.messages_map = self.hz_client.get_map("messages_map").blocking()
    def Log(self, request, context):
        msg_id = request.id
        msg = request.msg
        if self.messages_map.contains_key(msg_id):
            return logging_pb2.LogResponse(status="Duplicate message")
        self.messages_map.put(msg_id, msg)
        print(f"Logged: {msg_id} -> {msg}")
        return logging_pb2.LogResponse(status="Logged")
    
    def Health(self, request, context):
        return logging_pb2.HealthResponse(status="healthy")

    def GetLogs(self, request, context):
        return logging_pb2.GetLogsResponse(logs=" ".join(self.messages_map.values()))


def serve(port):

    c = consul.Consul()

    service_name = "logging-service"
    c.agent.service.register(
        name=service_name,
        service_id=str(random.randint(1, 100000)),
        port=port,

        check=consul.Check.tcp(
            host="localhost",
            port=port,
            interval="10s"
        )
    )
    index, data = c.kv.get('hazelcast_data')
    hazelcast_path = data['Value'].decode()[1:-1] if data and data['Value'] else None
    print(f"Hazelcast host: {hazelcast_path}")
    logging.getLogger("hazelcast").setLevel(logging.ERROR)
    hz_process = subprocess.Popen([hazelcast_path])

    def cleanup():
        hz_process.terminate()
        hz_process.wait()

    atexit.register(cleanup)
    hz = hazelcast.HazelcastClient()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingService(hz), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down server...")
    finally:
        hz.shutdown()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8001)
    serve(port=parser.parse_args().port)