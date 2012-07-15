import os
from pymongo import Connection
from dashgourd.plugins.cohort_funnel import create_cohort_funnel

connection = Connection(
    os.environ.get('MONGO_HOST', 'localhost'), 
    os.environ.get('MONGO_PORT', 27017))
db = connection[os.environ.get('MONGO_DB')]

mongo_user = os.environ.get('MONGO_USER')
mongo_pass = os.environ.get('MONGO_PASS')

collection = 'cohort_funnel'

query = {"gender": "Male", "actions": { "$exists": True}}

group = [
    {'meta': 'created_at', 'type': 'monthly'},
]

    calc = [
        {"type":"avg", "action":"listened_song"},
        {"type":"pct", "action":"listened_song"},
        {"type":"avg", "action":"bought_song"},
        {"type":"pct", "action":"bought_song"},
        {"type":"avg", "action":"listened_song", "meta": "time", "by":"listened_song"}    
    ] 

create_cohort_funnel(db, collection, query, group, calc)