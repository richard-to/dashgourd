class ChartsApi(object):
    """Api for generating data for charts
    
    Attributes:
        mongodb_db: Connected/Authenticated pymongo db object
    """
    
    def __init__(self, mongodb_db):
        self.db = mongodb_db
        
        