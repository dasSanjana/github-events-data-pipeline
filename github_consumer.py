from confluent_kafka import Consumer
import json

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "github-analytics",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["github-events"])

try:
    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Error:", msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))

        print("Event:", event["type"])
        print("User:", event["actor"]["login"])
        print("Repository:", event["repo"]["name"])
        print("-" * 40)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()