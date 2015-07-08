from pymongo import MongoClient

def init_mongodb(mongo_uri, mongo_dbname):
    conn = MongoClient(mongo_uri)
    return conn[mongo_dbname]
