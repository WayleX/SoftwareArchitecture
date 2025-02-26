import hazelcast
import time

client = hazelcast.HazelcastClient()

distributed_map = client.get_map("my-distributed-map").blocking()

for i in range(1000):
    distributed_map.put(i, f"Value-{i}")
    if i % 100 == 0:
        print(f"Inserted {i} keys...")

print("All 1000 keys inserted!")

time.sleep(5)

print("Check the Hazelcast Management Center for key distribution!")

client.shutdown()
