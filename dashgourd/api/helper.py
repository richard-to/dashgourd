import os
from pymongo import Connection
from actions import ActionsApi
from charts import ChartsApi

def get_mongodb_db():
    """ Gets connected mongodb db.
    
    Uses os environment variables to connect to mongo db 
    and get db used for dashgourd.
    
    Mainly a shortcut and is optional.
    
    Returns:
        mongo_db: Db used for dashgourd
    """
    
    connection = Connection(
        os.environ.get('MONGO_HOST', 'localhost'), 
        int(os.environ.get('MONGO_PORT', 27017)))
    mongo_db = connection[os.environ.get('MONGO_DB')]
    
    mongo_user = os.environ.get('MONGO_USER')
    mongo_pass = os.environ.get('MONGO_PASS')
    
    if mongo_user is not None and mongo_pass is not None:
        mongo_db.authenticate()
    
    return mongo_db

class HelperApi(object):
    """Helper function for initializing pymongo connection.
    
    This is merely a convenience class. Has problems of hard 
    coding the mongo db initializing and using global environment 
    variables.
    
    Api classes can be instantiated without this helper.
    """
    
    def __init__(self):

        self.db = get_mongodb_db()
        
        self.apis = {
            'actions': ActionsApi,
            'charts': ChartsApi
        }
        
    def get_api(self, name):
        """Gets chart api and initializes with mongo_db
        
        Returns:
            Initialized api (actions/charts currently) or None
        """
        
        api = self.apis.get(name)
        if api is None:
            return None
        return api(self.db)
