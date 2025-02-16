from concurrent import futures
import grpc
import logging_pb2
import logging_pb2_grpc

messages_store = {}

class LoggingService(logging_pb2_grpc.LoggingServiceServicer):
    def Log(self, request, context):
        msg_id = request.id
        msg = request.msg
        if msg_id in messages_store:
            return logging_pb2.LogResponse(status="Duplicate message")
        messages_store[msg_id] = msg
        print(f"Logged: {msg_id} -> {msg}")
        return logging_pb2.LogResponse(status="Logged")

    def GetLogs(self, request, context):
        return logging_pb2.GetLogsResponse(logs=" ".join(messages_store.values()))


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingService(), server)
    server.add_insecure_port('[::]:8001')
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()