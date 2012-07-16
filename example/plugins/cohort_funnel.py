from dashgourd.api.helper import HelperApi

collection = 'cohort_funnel'

query = {"gender": "Male", "actions": { "$exists": True}}

group = [
    {'meta': 'created_at', 'type': 'monthly'},
]

calc = [
    {"type":"avg", "action":"listened_song"},
    {"type":"pct", "action":"listened_song"},
    {"type":"avg", "action":["bought_song", "purchased_song"]},
    {"type":"pct", "action":"bought_song"},
    {"type":"avg", "action":"listened_song", "meta": "time", "by":"listened_song"}    
] 

helper_api = HelperApi()
chart_api = helper_api.get_api('charts')
chart_api.generate_chart('cohort_funnel', collection, 
    {'query':query, 'group':group, 'calc':calc})