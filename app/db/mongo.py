from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from app.core.config import settings
from typing import Optional


client: Optional[AsyncIOMotorClient]
_db: Optional[AsyncIOMotorDatabase]

async def connect()->None:
    global client,_db

    client = AsyncIOMotorClient(settings.MONGO_URI)
    _db=client[settings.MONGO_DB]

def get_db() -> AsyncIOMotorDatabase:
    if _db is None: raise RuntimeError("Mongo Down.")
    return _db

async def close()->None:
    global client, _db

    if client is not None: client.close()
    client=None
    _db=None

