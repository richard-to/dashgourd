from dashgourd.plugins.cohort_funnel import create_cohort_funnel
from dashgourd.plugins.user_retention import create_user_retention

class ChartsApi(object):
    """Api for charts.
    
    Api allows for creation and fetching of charts.
    
    Defaults with user retention and cohort funnel plugins.
    
    Attributes:
        mongodb_db: Connected/Authenticated pymongo db object
        plugins: Dict of plugins to load
    
    TODO(richard-to): Allow adding of new chart/map-reduce plugins to api
    """
    
    def __init__(self, mongodb_db, plugins=None):
        self.db = mongodb_db
        self.plugins = {
            'cohort_funnel':create_cohort_funnel,
            'user_retention':create_user_retention} 
      
    def get_chart(self, collection):      
        """Gets chart from DashGourd
        
        Gets chart data based on collection name.
        
        Data will not be returned in sorted order.
        
        Args:
            collection: Name of chart
        
        Returns: 
            PyMongo cursor of collection
            
        TODO(richard-to): Check if valid chart collection
        TODO(richard-to): Allow different export formats, such as csv
        """  
          
        return self.db[collection].find()
    
    def generate_chart(self, plugin, collection, options):
        """Generates chart based on plugin.
        
        This method is basically a wrapper for plugins.
        Plugin functions should be usable directly.
        
        Does not check what options are passed into plugin.
        Plugin needs to check that expected values are passed in.
        
        Currently errors handled silently.
        
        Args:
            plugin: Name of plugin
            collection: Name of collection
            options: Dict of options needed by plugin
        
        Returns:
            Chart plugin for generating chart data. Or returns False.
            
        TODO(richard-to): How should errors be handled?
        """
        
        plugin = self.plugins.get(plugin)
        if plugin is not None:
            return plugin(self.db, collection, options)
        else:
            return False