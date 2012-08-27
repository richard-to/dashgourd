from dashgourd.api.helper import init_mongodb
from dashgourd.charts.cohort_funnel import CohortFunnel
from dashgourd.charts.retention import Retention
from dashgourd.charts.action_cohort import ActionCohort

class ChartsApi(object):
    
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
        return self.db[collection].find()
    

    def generate_chart(self, plugin, collection, options):
        plugin = self.plugins.get(plugin)
        if plugin is not None:
            return plugin.run(self.db, collection, options)
        else:
            return False