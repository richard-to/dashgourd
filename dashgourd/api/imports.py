import re
from dashgourd.api.helper import init_mongodb

class ImportApi(object):
    """Api for managing direct imports to mongodb
    
    Sometimes it may not make sense to send each event 
    individually. For example if a cron job triggers 
    a couple hundred events, it may be better to import 
    directly to mongodb using a cron job versus sending 
    those events through a web api wrapper at that moment.
    
    It's less of problem if you are sending the events 
    directly to mongodb in bulk.
    
    For this to work, we need some way to manage what 
    events have been loaded and what have not.
    
    Attributes:
        mongodb: MongoDb db or Mongodb connection string
        dbname: If passing in connection string, dbname is needed
    """
    
    def __init__(self, mongodb, dbname=None):
        
        if type(mongodb) is str:
            self.db = init_mongodb(mongodb, dbname)
        else:
            self.db = mongodb
    
    def get_last_update(self, query_name):
        """Gets last update date for user/action/ab
        
        Args:
            query_name: Name of query to get last run/update date
        
        Returns:
            date: python DateTime object
        """
        
        result = None
        result = self.db.import_logs.find_one({"_id": query_name})
    
        if result is not None:
            return result['last_update']
        else:
            import_settings = self.db.settings.find_one({"_id": 'import'})
            self.db.import_logs.insert(
                {"_id": query_name, 'last_update': import_settings['start_date']})
            return import_settings['start_date']
        
    def set_last_update(self, query_name, last_update):
        """Registers an action type into the actions collection.
        
        The purpose of register action is mainly for the client 
        functionality. We may not actually need this.
            
        The action type will only be created if it does not exist.
        If no label is included with the action, a generic one 
        is generated from the `name` value by title-casing replacing 
        underscores with spaces.
        
        The action type should be in lowercase letters and contain no spaces.
        Digits and underscores are also allowed.
        
        Args:
            query_name: Name of query to track last run/update date
            last_update: Python date of last update
        """

        self.db.import_logs.update(
            {"_id": query_name}, 
            {'last_update': last_update}, 
            True)
        