import re

class EventApi(object):
    """Api for handling users/events
    
    Attributes:
        mongodb_db: Connected/Authenticated pymongo db object
    """
    def __init__(self, mongodb_db):
        self.db = mongodb_db
    
    """Creates a new user in DashGourd
    
    Embedded documents are allowed but not really supported yet.
    It's preferable to keep the user object flat with the 
    exception of the `_events` embedded document list
    
    When creating a user, no events can be included yet.
    
    Args:
        data: Dict that contains at least an `_id` key
    """    
    def create_user(self, data):

        if data['_id'] is not None:
            if data['_events'] is not None:
                del data['_events']
            self.db.users.insert(data)
    
    """Logs an event that a user triggered
    
    Events that have not been logged in the db
    are stored in an event collection.
    
    `_id` is used to find the user that triggered the event.
    
    `_type` is basically the slug name of the event.
    
    It is important to keep these slugs short. 
    The slug will be formatted to a more readable format or can be 
    overridden on the event table list table.
    
    Example: `user_registered` would become `User Registered`
    
    `created_at` is the date the event ocurred.
    
    
    Args:
        data: Dict that contains `_id`, `_type`, and `_created_at` keys
    """
            
    def insert_event(self, data):
        
        if (data['_id'] is not None and 
            data['_type'] is not None and
            data['_created_at'] is not None):
            _id = data['_id']
            del data['_id']
            self.create_event(data['_type'])
            self.db.users.update({ '_id':_id }, { '$push': { 'events': data } })
    
    """Creates or logs an event into the events collection.
    
    The event will only be created if it does not exist.
    If no label is included with the event, a generic one 
    is generated from the `_type` value by title-casing replacing 
    underscores with spaces.
    
    The event _type should be in lowercase letters and contain no spaces.
    Digits and underscores are also allowed.
    
    Args:
        _type: Name of the event type
    """        
    def create_event(self, _type, label=None):
        formatted_type = re.sub(r'[^a-z0-9_]', '', _type.lower())
        if formatted_type.len > 0:
            if self.db.events.find({"_type": formatted_type}).count() > 0:
                if label is None:
                    label = formatted_type.title()
                self.db.insert({"_type": formatted_type, 'label': label})
            