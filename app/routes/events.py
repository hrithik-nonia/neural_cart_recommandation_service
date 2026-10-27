from fastapi import APIRouter, Request, status, Depends
from app.utils.dependencies import get_current_user
from app.database import interactions_collection
from app.main import limiter
from datetime import datetime, timezone
from pydantic import BaseModel
from typing import Literal

router = APIRouter(prefix="/events", tags=["Events"])

# Event weights
WEIGHTS = {
    "view": 1,
    "click": 2,
    "purchase": 5,
}

class TrackEvent(BaseModel):
    product_id: str
    event_type: Literal["view", "click", "purchase"]


@router.post("/track", status_code=status.HTTP_201_CREATED)
@limiter.limit("30/minute")
async def track_event(
    request: Request,
    event_data: TrackEvent,
    user: dict = Depends(get_current_user)
):
    user_id = user.get("id")

    # Existing interaction check karo
    existing = await interactions_collection.find_one({
        "user_id": user_id,
        "product_id": event_data.product_id,
        "event_type": event_data.event_type,
    })

    if existing:
        # Weight badhao — baar baar dekha/kharida
        await interactions_collection.update_one(
            {"_id": existing["_id"]},
            {
                "$inc": {"weight": WEIGHTS[event_data.event_type]},
                "$set": {"updated_at": datetime.now(timezone.utc)}
            }
        )
    else:
        # Naya interaction save karo
        await interactions_collection.insert_one({
            "user_id": user_id,
            "product_id": event_data.product_id,
            "event_type": event_data.event_type,
            "weight": WEIGHTS[event_data.event_type],
            "created_at": datetime.now(timezone.utc),
        })

    return {"message": "Event track ho gaya"}