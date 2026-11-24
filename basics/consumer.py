from confluent_kafka import Consumer
import json

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-processing-group",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["orders"])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Error:", msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))

        print("Received event:", event)

except KeyboardInterrupt:
    print("Consumer stopped")

finally:
    consumer.close()