
import requests
import json
import time

from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

url = "https://api.github.com/events"

seen_events = set()

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(
            f"Event delivered: {msg.key().decode()} "
            f"to partition {msg.partition()}"
        )

while True:

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()

        events = response.json()

        for event in events:

            event_id = event["id"]

            if event_id in seen_events:
                continue

            seen_events.add(event_id)

            producer.produce(
                topic="github-events",
                key=str(event_id),
                value=json.dumps(event),
                callback=delivery_report
            )

        producer.flush()

        print("Fetched and published GitHub events")

    except requests.exceptions.RequestException as error:
        print("API error:", error)

    time.sleep(90)
