from confluent_kafka import Producer
import json

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

event = {
    "order_id": 101,
    "user_id": 25,
    "amount": 1499
}

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(
            f"Delivered to {msg.topic()}, "
            f"partition {msg.partition()}, "
            f"offset {msg.offset()}"
        )

producer.produce(
    topic="orders",
    key=str(event["user_id"]),
    value=json.dumps(event),
    callback=delivery_report
)

producer.flush()