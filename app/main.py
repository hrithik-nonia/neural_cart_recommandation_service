from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.database import client, interactions_collection, recommendations_collection
from app.redis import shared_redis
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

limiter = Limiter(key_func=get_remote_address)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # MongoDB check
    await client.admin.command("ping")
    print("✅ MongoDB connected")

    # Redis check
    await shared_redis.ping()
    print("✅ Redis connected")

    # Indexes
    await interactions_collection.create_index([("user_id", 1), ("product_id", 1)])
    await interactions_collection.create_index([("user_id", 1), ("event_type", 1)])
    await interactions_collection.create_index("created_at")
    await recommendations_collection.create_index("user_id", unique=True)
    print("✅ Indexes created")

    yield

    client.close()
    await shared_redis.aclose()
    print("🔴 Connections closed")


app = FastAPI(
    title="Recommendation API",
    description="Recommendation service for NeuralCart",
    version="1.0.0",
    docs_url="/recommendation-apis",
    lifespan=lifespan
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.get("/health")
async def health():
    return {"message": "Recommendation Service Running"}