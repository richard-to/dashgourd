from pymongo import Connection

def init_mongodb(mongo_uri, mongo_dbname):
    """Inits mongodb connection using connection string
    
    Args:
        mongodb_connection_string: Connection string with db name
    
    Return:
        db: Connected pymongo db instance
    """
    conn = Connection(mongo_uri)
    return conn[mongo_dbname]

    