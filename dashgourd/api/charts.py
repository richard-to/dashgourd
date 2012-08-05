from dashgourd.api.helper import init_mongodb
from dashgourd.charts.cohort_funnel import CohortFunnel
from dashgourd.charts.retention import Retention
from dashgourd.charts.action_cohort import ActionCohort

class ChartsApi(object):
    """Api for charts.
    
    Api allows for creation and fetching of charts.
        
    Attributes:
        mongodb: MongoDb db or Mongodb connection string
        dbname: If passing in connection string, dbname is needed
        plugins: Dict of chart plugins to load. If None, then defaults used
    """
    
    def __init__(self, mongodb, dbname=None, plugins=None):
        
        if type(mongodb) is str:
            self.db = init_mongodb(mongodb, dbname)
        else:
            self.db = mongodb
        
        if plugins is None:
            self.plugins = {
                'cohort_funnel':CohortFunnel(),
                'action_cohort': ActionCohort(),
                'retention': Retention()
            }
        else:
            self.plugins = plugins
      
    def get_chart(self, collection):      
        """Gets chart from DashGourd
        
        Gets chart data based on collection name.
        
        Data will not be returned in sorted order.
        
        Args:
            collection: Name of chart
        
        Returns: 
            PyMongo cursor of collection
            
        TODO(richard-to): Check if valid chart collection
        TODO(richard-to): Returning pymongo cursor probably not the best idea.
        """  
          
        return self.db[collection].find()
    
    def generate_chart(self, plugin, collection, options):
        """Generates chart based on plugin.
        
        This method is basically a wrapper for plugins.
        Plugin functions can be used directly.
        
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
            return plugin.run(self.db, collection, options)
        else:
            return False