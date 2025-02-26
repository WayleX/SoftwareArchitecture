import multiprocessing
import hazelcast
import time

def producer():
    client = hazelcast.HazelcastClient()
    queue = client.get_queue("bounded-queue").blocking()
    
    for i in range(1, 101):
        queue.put(i)
        print(f"Produced: {i}")
        time.sleep(0.01)


    client.shutdown()

def consumer(consumer_id):
    client = hazelcast.HazelcastClient()
    queue = client.get_queue("bounded-queue").blocking()
    
    while True:
        item = queue.take()
        print(f"Consumer {consumer_id} consumed: {item}")
        time.sleep(0.02)


def main():
    producer_process = multiprocessing.Process(target=producer)
    consumer_processes = [multiprocessing.Process(target=consumer, args=(i,)) for i in range(2)]
    
    producer_process.start()
    for p in consumer_processes:
        p.start()
    
    producer_process.join()
    for p in consumer_processes:
        p.join()

if __name__ == "__main__":
    main()
