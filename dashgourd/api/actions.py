import re
from dashgourd.api.helper import init_mongodb

class ActionsApi(object):
    """Api for handling users/actions
    
    
    Attributes:
        mongodb: MongoDb db or Mongodb connection string
        dbname: If passing in connection string, dbname is needed
    """
    
    def __init__(self, mongodb, dbname=None):
        
        if type(mongodb) is str:
            self.db = init_mongodb(mongodb, dbname)
        else:
            self.db = mongodb
            
        self.label_re = re.compile('[^a-z0-9_]')
       
    def create_user(self, data):
        """Creates a new user in DashGourd
        
        Embedded documents are allowed but not really supported yet.
        It's preferable to keep the user object flat with the 
        exception of the `actions` embedded document list
        
        When creating a user, no actions can be included yet.
        
        Make sure all created_at dates are converted to UTC before 
        inserting into database. Otherwise your dates will be converted 
        based on system timezone, which is not always ideal.
                
        Args:
            data: Dict that contains at least an `user_id` key
        """
         
        if 'user_id' in data:
            data['actions'] = []
            data['ab'] = {}
            self.db.users.insert(data)
                       
    def insert_action(self, user_id, data):
        """Logs a user action
        
        Actions that have not been logged in the db
        are stored in an action collection.
        
        `user_id` is used to find the user that did the action.
        
        `type` is basically the slug name of the action.
        
        See create_user method for details on why created_at fields 
        should be converted to UTC beforehand.
        
        Example: `user_registered` would become `User Registered`
        
        `created_at` is the date the action ocurred.
        
        
        Args:
            user_id: Id of user
            data: Dict that contains at least `name`, and `created_at` keys
        """        
        
        if ('name' in data and 
            'created_at' in data):            
            self.db.users.update({ 'user_id':user_id }, { '$push': { 'actions': data } })
    
    def tag_abtest(self, user_id, data): 
        """Tags as a user as being part of an ab test
        
        Ab tests are stored on the user object as a dictionary.
        
        Args:
            user_id: User id of user
            data: Dict that contains `abtest` `variation`
        """
        
        if ('abtest' in data and 
            'variation' in data):
            
            abtest = ".".join(['ab', data['abtest']])
            self.db.users.update(
                { 'user_id': user_id }, 
                { '$set': { abtest: data['variation'] } })       