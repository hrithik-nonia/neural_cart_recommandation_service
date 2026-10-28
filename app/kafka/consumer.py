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

        # Weights
        weights = {"view": 1, "click": 2, "purchase": 5}
        weight = weights.get(event_type, 1)

        # Interaction Redis mein increment karo
        key = f"interaction:{user_id}:{product_id}:{event_type}"
        await interaction_redis.incr(key, weight)
        await interaction_redis.expire(key, 24 * 60 * 60)  # 24 hours

        print(f"✅ Event saved: {key} = {weight}")