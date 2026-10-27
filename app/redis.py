# app/redis.py
import os
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

shared_redis = aioredis.from_url(
    os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    decode_responses=True
)

# Interaction Redis — clicks/views
interaction_redis = aioredis.from_url(
    os.getenv("INTERACTION_REDIS_URL", "redis://localhost:6380"),
    decode_responses=True
)