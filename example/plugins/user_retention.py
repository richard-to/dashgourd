import os
from pymongo import Connection
from dashgourd.plugins.user_retention import create_user_retention

connection = Connection(
    os.environ.get('MONGO_HOST', 'localhost'), 
    os.environ.get('MONGO_PORT', 27017))
db = connection[os.environ.get('MONGO_DB')]

mongo_user = os.environ.get('MONGO_USER')
mongo_pass = os.environ.get('MONGO_PASS')

collection = 'user_retention'
query = {"actions": { "$exists": True}}
group = [
    {'meta': 'created_at', 'type': 'monthly'}  
]
action = 'signedin'

create_user_retention(db, collection, query, group, action)
