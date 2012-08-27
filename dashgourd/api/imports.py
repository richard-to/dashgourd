import re
from dashgourd.api.helper import init_mongodb

class ImportApi(object):
    def __init__(self, mongodb, dbname=None):
        
        if type(mongodb) is str:
            self.db = init_mongodb(mongodb, dbname)
        else:
            self.db = mongodb
    
    
    def get_last_update(self, query_name):        
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
        self.db.import_logs.update(
            {"_id": query_name}, 
            {'last_update': last_update}, 
            True)
        