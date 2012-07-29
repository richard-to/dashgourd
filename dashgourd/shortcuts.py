import os
from dashgourd.api.charts import ChartsApi

def gen_chart(chart_plugin, collection, options):
    """Shortcut to initialize chart api and generate charts.
    
    This shortcut is useful for when you have many files that 
    build/generate map reduce chart data.
    
    This shortcut depends on environment variables for initializing 
    mongodb connections.
    
    Args:
        chart_plugin: Name of chart plugin to generate chart
        collection: Name of collection to be created
        options: Options associated with chart plugin
    """
    
    chart_api = ChartsApi(os.environ.get('MONGO_URI'), os.environ.get('MONGO_DB'))
    chart_api.generate_chart(chart_plugin, collection, options)