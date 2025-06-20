import os
import pymongo
import certifi
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')

def get_db():
    client = pymongo.MongoClient(MONGO_URI)
    return client[DB_NAME]
