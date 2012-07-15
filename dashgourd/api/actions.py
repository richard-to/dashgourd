import re

class ActionsApi(object):
    """Api for handling users/actions
    
    Attributes:
        mongodb_db: Connected/Authenticated pymongo db object
    """
    
    def __init__(self, mongodb_db):
        self.db = mongodb_db
        self.label_re = re.compile('[^a-z0-9_]')
            
    def create_user(self, data):
        """Creates a new user in DashGourd
        
        Embedded documents are allowed but not really supported yet.
        It's preferable to keep the user object flat with the 
        exception of the `actions` embedded document list
        
        When creating a user, no actions can be included yet.
        
        Args:
            data: Dict that contains at least an `_id` key
        """
         
        if '_id' in data:
            data['actions'] = []
            self.db.users.insert(data)
            
                
    def insert_action(self, data):
        """Logs a user action
        
        Actions that have not been logged in the db
        are stored in an action collection.
        
        `_id` is used to find the user that did the action.
        
        `type` is basically the slug name of the action.
        
        It is important to keep these slugs short. 
        The slug will be formatted to a more readable format or can be 
        overridden on the actions table list table.
        
        Example: `user_registered` would become `User Registered`
        
        `created_at` is the date the action ocurred.
        
        
        Args:
            data: Dict that contains `_id`, `name`, and `created_at` keys
        """        
        
        if ('_id' in data and 
            'name' in data and 
            'created_at' in data):
            
            _id = data['_id']
            del data['_id']
            
            self.db.users.update({ '_id':_id }, { '$push': { 'actions': data } })
    
    
    def register_action(self, name, label=None):
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
            name: Name of the action
        """
         
        formatted_name = self.label_re.sub('',name.lower())
        if formatted_name:
            if not label:
                label = formatted_name.replace('_', ' ', ).title()
            self.db.actions.update({"name": formatted_name}, 
                {"name": formatted_name, 'label': label}, True)
            