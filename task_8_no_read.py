import multiprocessing
import hazelcast
import time

def producer():
    client = hazelcast.HazelcastClient()
    queue = client.get_queue("bounded-queue").blocking()
    
    for i in range(1, 101):
        queue.put(i)
        print(f"Produced: {i}")


    client.shutdown()

def main():
    producer_process = multiprocessing.Process(target=producer)
    
    producer_process.start()
    
    producer_process.join()


if __name__ == "__main__":
    main()
