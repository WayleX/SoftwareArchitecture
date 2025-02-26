import multiprocessing
import hazelcast
import time

def increment_shared_map_pessimistic(client, key, iterations=10_000):
    map = client.get_map("distributed-map").blocking()
    map.put_if_absent(key, 0)
    for _ in range(iterations):
        while True:
            map.lock(key)
            try:
                value = map.get(key)
                value += 1
                map.put(key, value)
                break
            finally:
                map.unlock(key)

def client_process(key):
    client = hazelcast.HazelcastClient()
    increment_shared_map_pessimistic(client, key)
    client.shutdown()

def main():
    key = "key_name_5"
    processes = [multiprocessing.Process(target=client_process, args=(key,)) for _ in range(3)]
    
    start_time = time.time()
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    print(f"Execution time: {time.time() - start_time:.4} seconds")

    client = hazelcast.HazelcastClient()
    map = client.get_map("distributed-map").blocking()
    print(f"Final value of 'key': {map.get(key)}")
    client.shutdown()

if __name__ == "__main__":
    main()
