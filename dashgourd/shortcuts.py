import os
from dashgourd.api.charts import ChartsApi

def gen_chart(chart_plugin, collection, options):    
    chart_api = ChartsApi(os.environ.get('MONGO_URI'), os.environ.get('MONGO_DB'))
    chart_api.generate_chart(chart_plugin, collection, options)