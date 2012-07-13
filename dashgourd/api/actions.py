import re

class ActionsApi(object):
    """Api for handling users/actions
    
    Attributes:
        mongodb_db: Connected/Authenticated pymongo db object
    """
    
    def __init__(self, mongodb_db):
        self.db = mongodb_db
    
    """Creates a new user in DashGourd
    
    Embedded documents are allowed but not really supported yet.
    It's preferable to keep the user object flat with the 
    exception of the `_actions` embedded document list
    
    When creating a user, no actions can be included yet.
    
    Args:
        data: Dict that contains at least an `_id` key
    """    
    
    def create_user(self, data):

        if data['_id'] is not None:
            if data['_actions'] is not None:
                del data['_actions']
            self.db.users.insert(data)
    
    """Logs a user action
    
    Actions that have not been logged in the db
    are stored in an action collection.
    
    `_id` is used to find the user that did the action.
    
    `_type` is basically the slug name of the action.
    
    It is important to keep these slugs short. 
    The slug will be formatted to a more readable format or can be 
    overridden on the actions table list table.
    
    Example: `user_registered` would become `User Registered`
    
    `created_at` is the date the action ocurred.
    
    
    Args:
        data: Dict that contains `_id`, `_type`, and `_created_at` keys
    """
            
    def insert_action(self, data):
        
        if (data['_id'] is not None and 
            data['_type'] is not None and
            data['_created_at'] is not None):
            
            _id = data['_id']
            del data['_id']
            
            _label = None
            if data['_label'] is not None:
                _label = data['_label']
            del data['_label']
            
            self.recognize_action(data['_type'], _label)
            self.db.users.update({ '_id':_id }, { '$push': { '_actions': data } })
    
    """Creates or logs an action type into the actions collection.
    
    The action type will only be created if it does not exist.
    If no label is included with the action, a generic one 
    is generated from the `_type` value by title-casing replacing 
    underscores with spaces.
    
    The action _type should be in lowercase letters and contain no spaces.
    Digits and underscores are also allowed.
    
    Args:
        _type: Name of the action type
    """        
    
    def recognize_action(self, _type, _label=None):
        formatted_type = re.sub(r'[^a-z0-9_]', '', _type.lower())
        if formatted_type.len > 0:
            if self.db.actions.find({"_type": formatted_type}).count() > 0:
                if _label is None:
                    _label = formatted_type.title()
                self.db.actions.insert({"_type": formatted_type, '_label': _label})
            