from motor import motor_asyncio

from config import settings


client = motor_asyncio.AsyncIOMotorClient(settings.MONGODB_URL)


def get_collection():
    return client.sampleDB.sample_collection
