from dashgourd.charts.cohort_funnel import CohortFunnel

class ActionCohort(CohortFunnel):
    def __init__(self):
        super(ActionCohort, self).__init__()
        self.mapper_template = """ 
        function() {{

            var nums = [
                "01", "02", "03", "04", "05",
                "06", "07", "08", "09", "10",
                "11", "12", "13", "14", "15",
                "16", "17", "18", "19", "20",
                "21", "22", "23", "24", "25",
                "26", "27", "28", "29", "30", 
                "31"            
            ];
            
            var values = {{}}
            
            {set_emit_keys}
            
            var emit_keys = {{
                {init_emit_keys}
            }}
            
            this.actions.forEach(function(z){{
                
                if({init_values_cond}){{
                    if(values[z.{pivot_key}] == undefined){{
                        values[z.{pivot_key}] = {{
                            total:{{'value':0, calc:'sum'}},
                            {init_values}                       
                        }};
                    }}
                }}
                
                {set_action_values}
            
            }});
            
            for(key in values) {{
            
                {adjust_cond_values}
                
                emit_keys.{event_emit_key} = values[key].{event_emit_key};
                emit(emit_keys, values[key]);
            }}        
        }}
        """          


    def run(self, db, collection, options):

        debug = options.get('debug', False)
        output = options.get('output', 'replace')
        
        query = options.get('query')
        pivot = options.get('pivot')
        group = options.get('group')
        calc = options.get('calc')
        
        if pivot is None or query is None or group is None or calc is None:
            return False

        group = self.validate_group_config(group)
        calc = self.validate_calc_config(calc)

        set_emit_keys = self.build_set_emit_keys(group)
        init_emit_keys = self.build_init_emit_keys(group)
        init_values_cond = self.build_init_values_cond(
            calc, self.build_init_values_cond(group))
        init_values = self.build_init_values(calc)
        set_action_values = self.build_init_pivot_values(pivot, group)        
        set_action_values = self.build_set_pivot_action_values(
            pivot, calc, set_action_values)
        adjust_cond_values = self.build_adjust_pivot_cond_values(calc)
        init_final_values = self.build_init_final_values(calc)        
        set_final_calc = self.build_init_final_calc(calc)

        out_pivot_key = pivot

        out_set_emit_keys = self.gen_set_emit_keys(set_emit_keys)
        out_init_emit_keys = self.gen_init_emit_keys(init_emit_keys)
        out_event_emit_key = self.gen_event_emit_key(group)
      
        out_init_values_cond = self.gen_init_values_cond(init_values_cond)               
        out_init_values = self.gen_init_values(init_values)
        out_set_action_values = self.gen_set_action_values(set_action_values)
        out_adjust_cond_values = self.gen_adjust_cond_values(adjust_cond_values)

        out_set_reduce_sum = self.gen_set_reduce_sum(init_values)
        
        out_init_final_values = self.gen_init_final_values(init_final_values)
        out_set_final_calc = self.gen_set_final_calc(set_final_calc)
    
        mapper = self.mapper_template.format(
            set_emit_keys=out_set_emit_keys, 
            init_emit_keys=out_init_emit_keys,
            event_emit_key=out_event_emit_key,
            pivot_key=out_pivot_key,
            init_values_cond=out_init_values_cond,
            init_values=out_init_values,
            set_action_values=out_set_action_values,
            adjust_cond_values=out_adjust_cond_values)
        
        reducer = self.reducer_template.format(out_init_values, out_set_reduce_sum)

        finalizer = self.finalizer_template.format(out_init_final_values, out_set_final_calc)

        if debug:
            self.debug(mapper, reducer, finalizer)
        else:
            self.build_chart(db, output, collection, mapper, 
                reducer, finalizer, query)

    def build_init_pivot_values(self, pivot, group, set_action_values={}):
        
        for data in group:
            if data['type'] == 'action':
                if data['format'] == 'value':
                    set_code = 'values[z.{n}].{m} = z.{m}; '.format(n=pivot, m=data['meta'])
                elif data['format'] == 'monthly':
                    set_code = 'values[z.{n}].{m} = z.{m}.getFullYear() + "/" + nums[z.{m}.getMonth()] + "/01"; '.format(
                        n=pivot, m=data['meta'])
                elif data['format'] == 'weekly':
                    set_code = ("z.{m}.setDate(z.{m}.getDate() - z.{m}.getDay()); "
                "values[z.{n}].{m} = z.{m}.getFullYear() + '/' + nums[z.{m}.getMonth()] + '/' + nums[z.{m}.getDate()-1]; ").format(
                    n=pivot, m=data['attr'])
                set_count = "values[z.{}].total.value = 1;".format(pivot)
                
                code = " ".join([set_count, set_code])

                if type(data['attr']) is list:
                    action_list = data['attr']
                else:
                    action_list = [data['attr']]
                
                for action in action_list:   
                    if action not in set_action_values:
                        set_action_values[action] = {}
                                       
                    set_action_values[action][action] = code
        return set_action_values

    def build_init_values_cond(self, config, init_values_cond=[]):
        
        for data in config:
            if data['type'] == 'action':
                if type(data['attr']) is list:
                    init_values_cond.extend(data['attr'])
                else:
                    init_values_cond.append(data['attr'])
        return init_values_cond

    def build_set_pivot_action_values(self, pivot, calc, set_action_values={}):
        for data in calc:
            if data['type'] == 'action':
                if 'meta' in data:
                    code = "if(z.{m} != undefined){{ values[z.{p}].{n}.value += z.{m}; }}".format(
                        p=pivot, m=data['meta'], n=data['name'])
                elif 'meta' in data and 'value' in data:
                    code = "if(z.{m} != undefined && z.{m} == '{v}'){{ values[z.{p}].{n}.value++; }}".format(
                        p=pivot, m=data['meta'], n=data['name'], v=data['value'])
                else:
                    code = "values[z.{}].{}.value++;".format(pivot, data['name'])
        
                if type(data['attr']) is list:
                    action_list = data['attr']
                else:
                    action_list = [data['attr']]
                
                for action in action_list:   
                    if action not in set_action_values:
                        set_action_values[action] = {}
                                       
                    set_action_values[action][data['name']] = code
        return set_action_values

    def build_adjust_pivot_cond_values(self, calc, adjust_cond_values=[]):
        for data in calc:
            if data['calc'] == 'pct' and data['cond']['type'] != 'sum':
                op = self.accepted_conditions[data['cond']['type']]            
                cond_code = ("values[key].{n}.value = (values[key].{n}.value {op} {v}) ? 1 : 0; ").format(
                    n=data['name'], op=op, v=data['cond']['value'])
                adjust_cond_values.append(cond_code)
        return adjust_cond_values

    def gen_init_values_cond(self, init_values_cond):
        return " || ".join(["z.name == '{}'".format(action) for action in init_values_cond])

    def gen_event_emit_key(self, group):
        for data in group:
            if data['type'] == 'action':
                return data['meta'] 