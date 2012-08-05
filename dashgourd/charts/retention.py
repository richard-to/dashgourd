import math
from datetime import datetime
from bson.code import Code

from dashgourd.charts.cohort_funnel import CohortFunnel

class Retention(CohortFunnel):
    def __init__(self):
        super(Retention, self).__init__()
        self.mapper_template = """
        function(){{

            var nums = [
                "01", "02", "03", "04", "05",
                "06", "07", "08", "09", "10",
                "11", "12", "13", "14", "15",
                "16", "17", "18", "19", "20",
                "21", "22", "23", "24", "25",
                "26", "27", "28", "29", "30", 
                "31"            
            ];
            
            {set_emit_keys}
            
            var emit_keys = {{
                {init_emit_keys}
            }}

            var startDateStr = {emit_date_field};   
            var startDate = new Date(startDateStr);
            
            var values = {{}};
            values[startDateStr] = {{total: 1}};
            
            this.actions.forEach(function(z){{
                if(z.name == '{action_name}'){{
                    
                    {set_interval_code}
                    
                    if(z.created_at >= startDate && values[createdDateStr] == undefined){{
                        values[createdDateStr] = {{total: 1}};
                    }}
                }}
            }});       
            emit(emit_keys, values);        
        }}
        """
        
        self.reducer_template = """    
        function(key, values){
            var result = {}
                  
            values.forEach(function(value){
                for(key in value){
                    if(result[key]){
                        result[key].total += value[key].total;
                    } else {
                        result[key] = {total: value[key].total};
                    }
                }
            });
            return result;
        }
        """
        
        self.finalizer_template = """
        function(key, value){{
            startDate = key.{};
            var startTotal = value[startDate].total;
            if(startTotal > 0){{
                for(date in value){{
                    value[date].pct = {{
                        calc: 'pct',
                        value: value[date].total/startTotal,
                        total: value[date].total,
                        n: startTotal
                    }}
                }}
            }}
            return value;
        }}
        """

    def run(self, db, collection, options):

        debug = options.get('debug', False)
        output = options.get('output', 'replace')
    
        query = options.get('query')
        group = options.get('group')
        action = options.get('action')
    
        if query is None or group is None or action is None:
            return False

        group = self.validate_group_config(group)
        set_emit_keys = self.build_set_emit_keys(group)
        init_emit_keys = self.build_init_emit_keys(group)

        out_set_emit_keys = self.gen_set_emit_keys(set_emit_keys)
        out_init_emit_keys = self.gen_init_emit_keys(init_emit_keys)
        out_emit_date_field = self.out_emit_data_field(group)
        out_set_interval_code = self.out_set_interval_code(group)
        out_action = action

        mapper = self.mapper_template.format(
            set_emit_keys=out_set_emit_keys,
            init_emit_keys=out_init_emit_keys,            
            emit_date_field=out_emit_date_field,
            set_interval_code=out_set_interval_code,
            action_name=out_action)
        reducer = self.reducer_template
        finalizer = self.finalizer_template.format(out_emit_date_field)

        if debug:
            self.debug(mapper, reducer, finalizer)
        else:
            self.build_chart(db, output, collection, mapper, 
                reducer, finalizer, query)

    def out_set_interval_code(self, group):
        interval_code = ''
        for data in group:
            if data['format'] == 'monthly':
                interval_code = ("var createdDateStr = z.created_at.getFullYear() + '/' "
                    "+ nums[z.created_at.getMonth()] + '/01'; "
                    "z.created_at.setDate(1);")
                break
            elif data['format'] == 'weekly':
                interval_code = ("z.created_at.setDate(z.created_at.getDate() "
                    " - z.created_at.getDay()); "
                    "var createdDateStr = z.created_at.getFullYear() + '/' + " 
                    "nums[z.created_at.getMonth()] + '/' + nums[z.created_at.getDate()-1];")
                break
        return interval_code
        
    def out_emit_data_field(self, group):
        for data in group:
            if data['format'] == 'monthly' or data['format'] == 'weekly':
                return data['attr']