import os
from pymongo import Connection
from actions import ActionsApi
from charts import ChartsApi

class HelperApi(object):
    """Helper function for initializing pymongo connection.
    
    This is merely a convenience class. Has problems of hard 
    coding the mongo db initializing and using global environment 
    variables.
    
    Api classes can be instantiated without this helper.
    
    """
    
    def __init__(self):
        connection = Connection(
            os.environ.get('MONGO_HOST', 'localhost'), 
            os.environ.get('MONGO_PORT', 27017))
        mongo_db = connection[os.environ.get('MONGO_DB')]
        
        mongo_user = os.environ.get('MONGO_USER')
        mongo_pass = os.environ.get('MONGO_PASS')
        
        if mongo_user is not None and mongo_pass is not None:
            mongo_db.authenticate()
        
        self.db = mongo_db
        
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