import json
import time
import random
import uuid
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker()

# ─── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC        = "social-events"
EVENTS_PER_SECOND = 50  # ~3,000/min to start (safe for local machine)

# ─── Realistic Data ────────────────────────────────────────────────────────────
HASHTAGS = [
    "Coachella", "WorldCup", "AI", "Python", "DataEngineering",
    "Spark", "Kafka", "Music", "Food", "Travel", "Tech", "Gaming",
    "NBA", "Netflix", "Crypto", "ClimateChange", "Fashion", "Fitness"
]

EVENT_TYPES = ["post", "like", "share", "comment", "repost"]
EVENT_WEIGHTS = [0.35, 0.30, 0.15, 0.15, 0.05]  # posts most common

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.65, 0.25, 0.10]

LOCATIONS = [
    "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX",
    "London, UK", "Tokyo, Japan", "Sydney, Australia", "Berlin, Germany",
    "Paris, France", "Toronto, Canada", "São Paulo, Brazil", "Mumbai, India"
]

# ─── Event Generator ───────────────────────────────────────────────────────────
def generate_event():
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    hashtags   = random.sample(HASHTAGS, k=random.randint(1, 4))

    event = {
        "event_id":   str(uuid.uuid4()),
        "event_type": event_type,
        "user_id":    f"user_{random.randint(1000, 99999)}",
        "username":   fake.user_name(),
        "content":    fake.sentence(nb_words=12) + " " + " ".join(f"#{h}" for h in hashtags),
        "hashtags":   hashtags,
        "likes":      random.randint(0, 10000)  if event_type == "post"    else 0,
        "shares":     random.randint(0, 5000)   if event_type == "post"    else 0,
        "comments":   random.randint(0, 2000)   if event_type == "post"    else 0,
        "location":   random.choice(LOCATIONS),
        "device":     random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
        "timestamp":  datetime.utcnow().isoformat(),
    }
    return event

# ─── Kafka Producer ────────────────────────────────────────────────────────────
def create_producer(retries=5, delay=3):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",              # wait for broker ack
                retries=3,
                linger_ms=10,            # small batching for throughput
                batch_size=16384,
            )
            print(f"✅ Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"⏳ Kafka not ready, retrying ({attempt}/{retries})...")
            time.sleep(delay)
    raise RuntimeError("❌ Could not connect to Kafka after retries")

# ─── Main Loop ─────────────────────────────────────────────────────────────────
def main():
    producer = create_producer()
    total    = 0
    interval = 1.0 / EVENTS_PER_SECOND

    print(f"🚀 Streaming social events to topic '{TOPIC}' at ~{EVENTS_PER_SECOND * 60:,}/min")
    print("   Press Ctrl+C to stop.\n")

    try:
        while True:
            event = generate_event()
            producer.send(TOPIC, value=event)
            total += 1

            # Print a sample every 500 events
            if total % 500 == 0:
                print(f"📨 Sent {total:,} events | Latest: [{event['event_type']}] "
                      f"{event['username']} → {event['hashtags']}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n⛔ Stopped. Total events sent: {total:,}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
