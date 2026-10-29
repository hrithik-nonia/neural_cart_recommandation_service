from aiokafka import AIOKafkaConsumer
import json
import os
from app.redis import interaction_redis

consumer = None

async def start_consumer():
    global consumer
    consumer = AIOKafkaConsumer(
        "product-views",
        "product-purchases",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id="recommendation-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    await consumer.start()
    print("✅ Kafka Consumer started")


async def stop_consumer():
    global consumer
    if consumer:
        await consumer.stop()
        print("🔴 Kafka Consumer stopped")


async def consume_events():
    async for msg in consumer:
        event = msg.value
        user_id = event.get("user_id")
        product_id = event.get("product_id")
        event_type = event.get("event_type")

        if not user_id or not product_id:
            continue

        weights = {"view": 1, "click": 2, "purchase": 5}
        weight = weights.get(event_type, 1)

        # 1. Product views global sorted set
        await interaction_redis.zincrby(
            "product:views",           # ← global sorted set
            weight,                    # ← score increment
            product_id                 # ← member
        )

        # 2. User-specific interaction
        await interaction_redis.zincrby(
            f"user:interactions:{user_id}",  # ← user ka sorted set
            weight,
            product_id
        )

        # 3. TTL set karo
        await interaction_redis.expire("product:views", 7 * 24 * 60 * 60)        # 7 days
        await interaction_redis.expire(f"user:interactions:{user_id}", 24 * 60 * 60)  # 24 hours

        print(f"✅ Event saved = {weight}")