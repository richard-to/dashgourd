from pymongo import Connection

def init_mongodb(mongo_uri, mongo_dbname):
    conn = Connection(mongo_uri)
    return conn[mongo_dbname]