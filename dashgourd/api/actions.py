import copy
from dashgourd.api.helper import init_mongodb

class ActionsApi(object):

    def __init__(self, mongodb, dbname=None):
        if type(mongodb) is str:
            self.db = init_mongodb(mongodb, dbname)
        else:
            self.db = mongodb
    

    def create_user(self, data):
        if ('_id' in data and 
            'created_at' in data):
            self.db.users.insert(data)


    def update_profile(self, id, data):
        self.db.users.update({'_id':id}, {'$set': data})


    def insert_action(self, id, data, unique=False):
        if ('name' in data and 
            'created_at' in data):

            if unique is True:
                insert_values = copy.copy(data)
                del insert_values['created_at']
                user = self.db.users.find_one({ 
                    '_id' : id,
                    'actions':{'$elemMatch': insert_values}}, {'_id': 1})
                if user:
                    return
            self.db.users.update({'_id':id}, {'$push': {'actions': data}})
    

    def tag_abtest(self, id, data): 
        if ('abtest' in data and 
            'variation' in data):
            
            abtest = ".".join(['ab', data['abtest']])
            self.db.users.update(
                {'_id': id}, 
                {'$set': {abtest: data['variation']}})       