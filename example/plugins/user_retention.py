from dashgourd.api.helper import HelperApi

collection = 'user_retention'
query = {"actions": { "$exists": True}}
group = [
    {'meta': 'created_at', 'type': 'monthly'}  
]
action = 'signedin'

helper_api = HelperApi()
chart_api = helper_api.get_api('charts')
chart_api.generate_chart('user_retention', collection, 
    {'query':query, 'group':group, 'action':action})
