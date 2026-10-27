import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

client = AsyncIOMotorClient(os.getenv("MONGODB_URI"))
db = client[os.getenv("DATABASE_NAME")]

# Collections
interactions_collection = db["interactions"]  # user ka view/purchase history
recommendations_collection = db["recommendations"]  # cached recommendations