import re
from bson.code import Code

class CohortFunnel(object):
    
    def __init__(self):
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
            
            var values = {{
                total: {{value:1, calc:'sum'}},
                {init_values}
            }}
            
            {set_emit_keys}
            
            var emit_keys = {{
                {init_emit_keys}
            }}

            {set_user_values}

            if(this.actions){{
                this.actions.forEach(function(z){{
                    {set_action_values}                       
                }});
            }}
            
            {set_bucket_values}

            {adjust_cond_values}
            
            emit(emit_keys, values);        
        }}
        """
            
        self.reducer_template = """    
        function(key, values){{
            var result = {{
                total: {{value:0, calc:'sum'}},
                {}
            }}
                  
            values.forEach(function(value) {{
                result.total.value += value.total.value;
                {}
            }});

            return result;
        }}
        """
        
        self.finalizer_template = """
        function(key, value){{

            {}

            {}
            
            return value;
        }}
        """

        self.accepted_conditions = {
            'at_least': '>=',
            'at_most': '<=',
            'exactly': '==',
            'if': None
        }

    def run(self, db, collection, options):

        debug = options.get('debug', False)
        output = options.get('output', 'replace')
        
        query = options.get('query')
        group = options.get('group')
        calc = options.get('calc')
        
        if query is None or group is None or calc is None:
            return False

        group = self.validate_group_config(group)
        calc = self.validate_calc_config(calc)
        
        set_emit_keys = self.build_set_emit_keys(group)
        init_emit_keys = self.build_init_emit_keys(group)

        init_values = self.build_init_values(calc)
        set_user_values = self.build_set_user_values(calc)
        set_action_values = self.build_set_action_values(calc)       

        calc, init_values, set_bucket_values = self.build_set_range_bucket_values(
            calc, init_values)

        adjust_cond_values = self.build_adjust_cond_values(calc)

        init_final_values = self.build_init_final_values(calc)        
        set_final_calc = self.build_init_final_calc(calc)
        
        out_set_emit_keys = self.gen_set_emit_keys(set_emit_keys)
        out_init_emit_keys = self.gen_init_emit_keys(init_emit_keys)
        
        out_init_values = self.gen_init_values(init_values)
        out_set_user_values = self.gen_set_user_values(set_user_values)
        out_set_action_values = self.gen_set_action_values(set_action_values)
        out_adjust_cond_values = self.gen_adjust_cond_values(adjust_cond_values)
        out_set_bucket_values = self.gen_set_bucket_values(set_bucket_values)

        out_set_reduce_sum = self.gen_set_reduce_sum(init_values)
        
        out_init_final_values = self.gen_init_final_values(init_final_values)
        out_set_final_calc = self.gen_set_final_calc(set_final_calc)

        mapper = self.mapper_template.format(
            set_emit_keys=out_set_emit_keys, 
            init_emit_keys=out_init_emit_keys,
            init_values=out_init_values,
            set_user_values=out_set_user_values,
            set_action_values=out_set_action_values,
            set_bucket_values=out_set_bucket_values,
            adjust_cond_values=out_adjust_cond_values)
        
        reducer = self.reducer_template.format(out_init_values, out_set_reduce_sum)

        finalizer = self.finalizer_template.format(out_init_final_values, out_set_final_calc)

        if debug:
            self.debug(mapper, reducer, finalizer)
        else:
            self.build_chart(db, output, collection, mapper, 
                reducer, finalizer, query)

    def validate_group_config(self, group):
        validated_group = []
        for data in group:
            
            if 'type' not in data:
                data['type'] = 'user'
            
            if 'format' not in data:
                data['format'] = 'value'
           
            validated_group.append(data)
        return validated_group
         
    def build_set_emit_keys(self, group, set_emit_keys=[]):      
        for data in group:
            
            if data['type'] == 'user':
                if data['format'] == 'value':
                    emit_key_part = 'var {a} = this.{a};'.format(a=data['attr'])
                elif data['format'] == 'monthly':
                    emit_key_part = ("var {a} = this.{a}.getFullYear() + '/' + "
                     "nums[this.{a}.getMonth()] + '/01';").format(a=data['attr'])
                elif data['format'] == 'weekly':
                    emit_key_part = ("this.{a}.setDate(this.{a}.getDate() - "
                        "this.{a}.getDay()); var {a} = this.{a}.getFullYear() + '/' "
                        " + nums[this.{a}.getMonth()] + '/' " + 
                        "+ nums[this.{a}.getDate()-1];").format(a=data['attr'])
            elif data['type'] == 'ab':
                if data['format'] == 'value':
                    emit_key_part = 'var {a} = "variation_" + parseInt(this.ab.{a});'.format(a=data['attr'])
            
            if data['type'] == 'user' or data['type'] == 'ab':
                set_emit_keys.append(emit_key_part)                   
        return set_emit_keys

    def build_init_emit_keys(self, group, init_emit_keys=[]):        

        for data in group:
            if data['type'] == 'user' or data['type'] == 'ab':
                init_emit_keys.append("{a}:{a}".format(a=data['attr']))
        
        return init_emit_keys

    def validate_calc_config(self, calc):
        validated_calc = []
        
        default_type = 'action'

        for data in calc:
            
            if 'type' not in data:
                data['type'] = default_type

            if 'n' not in data:
                data['n'] = 'total'

            cond = {'type': 'sum', 'value': 1}
            if type(data.get('cond')) is dict:
                if data['cond'].get('type') in self.accepted_conditions:
                    cond['type'] = data['cond']['type']
                    
                if  data['cond'].get('value') is not None:
                    cond['value'] = data['cond']['value']

            data['cond'] = cond

            if 'name' not in data:
                
                if type(data['attr']) is list:
                    data['name'] = "_or_".join(data['attr'])
                else:
                    data['name'] = data['attr']

                if  data['calc'] == 'pct' and data['cond']['type'] != 'sum':               
                    data['name'] = "{}_{}_{}_{}".format(
                        'has', data['cond']['type'], data['cond']['value'], data['name'])

                if 'meta' in data:
                    data['name'] = "{}_{}".format(data['name'], data['meta'])

                if 'value' in data:
                    label = data['value'].replace(' ', '_').lower()
                    data['name'] = "{}_{}".format(data['name'], label)

                if data['cond']['type'] == 'if':
                    data['name'] = "{}_if_{}".format(data['name'], data['cond']['value'])
            validated_calc.append(data)
        
        return validated_calc

    def build_init_values(self, calc, init_values=[]):
        for data in calc:
            if data['name'] not in init_values:    
                init_values.append(data['name'])
        return init_values

    def build_set_user_values(self, calc, set_user_values=[]):
        for data in calc:
            if data['type'] == 'user' and 'value' in data:
                code = "if(this.{a} == '{v}'){{ values.{n}.value++; }}".format(
                    a=data['attr'], n=data['name'], v=data['value'])
                set_user_values.append(code)
        return set_user_values

    def build_set_action_values(self, calc, set_action_values={}):
        for data in calc:
            if data['type'] == 'action':
                if 'meta' in data and 'value' in data:
                    code = "if(z.{m} != undefined && z.{m} == '{v}'){{ values.{n}.value++; }}".format(
                        m=data['meta'], n=data['name'], v=data['value'])
                elif 'meta' in data:
                    code = "if(z.{m} != undefined){{ values.{n}.value += z.{m}; }}".format(
                        m=data['meta'], n=data['name'])                
                else:
                    code = "values.{}.value++;".format(data['name'])
        
                if type(data['attr']) is list:
                    action_list = data['attr']
                else:
                    action_list = [data['attr']]
                
                for action in action_list:   
                    if action not in set_action_values:
                        set_action_values[action] = {}
                                       
                    set_action_values[action][data['name']] = code
        
        return set_action_values

    def build_adjust_cond_values(self, calc, adjust_cond_values=[]):
        for data in calc:
            if data['calc'] == 'pct' and data['cond']['type'] != 'sum':
                op = self.accepted_conditions[data['cond']['type']]            
                cond_code = ("values.{n}.value = (values.{n}.value {op} {v}) ? 1 : 0; ").format(
                    n=data['name'], op=op, v=data['cond']['value'])
                adjust_cond_values.append(cond_code)
            elif data['cond']['type'] == 'if':
                cond_code = ("values.{n}.value = "
                    "(values.{n}.value > 0 && values.{if_cond}.value > 0) "
                    "? values.{n}.value : 0; ").format(n=data['name'], if_cond=data['cond']['value'])
                adjust_cond_values.append(cond_code)  
        return adjust_cond_values

    def build_set_range_bucket_values(self, calc, init_values=[], set_bucket_values=[]):
        new_calc = []
        for data in calc:
            bucket = data.get('bucket', None)    
            if bucket is not None and bucket['type'] == 'range':

                for value in bucket['value']:
                    if type(value) is int:
                        new_name = "{n}_{v}".format(n=data['name'], v=value)
                        cond = ("if(values.{n}.value == {v}){{ " +
                        "values.{n}_{v}.value = 1; " + 
                        "}}").format(n=data['name'], v=value)
                    elif len(value) == 2 and value[1] is None:
                        new_name = "{n}_{m}_plus".format(
                            n=data['name'], m=value[0])
                        cond = ("if(values.{n}.value >= {m}){{ " +
                        "values.{n}_{m}_plus.value = 1; " + 
                        "}}").format(n=data['name'], m=value[0])
                    else:
                        new_name = "{n}_{m}_to_{x}".format(
                            n=data['name'],  m=value[0], x=value[1])
                        cond = ("if(values.{n}.value >= {m} && values.{n}.value <= {x}){{ "
                        "values.{n}_{m}_to_{x}.value = 1;" + 
                        "}}").format(n=data['name'], m=value[0], x=value[1])
                    calc_config = {
                        'calc': data['calc'], 
                        'attr': data['attr'],
                        'name': new_name,
                        'n': data['n'], 
                        'type': data['type'],
                        'cond': data['cond']   
                    }
                    new_calc.append(calc_config)
                    
                    init_values.append(new_name)
                    set_bucket_values.append(cond)

        calc.extend(new_calc)
        return [calc, init_values, set_bucket_values]         

    def build_init_final_values(self, calc, init_final_values=[]):
        for data in calc:
            if data['calc'] != 'sum':
                data['calc_name'] = "{}_{}".format(data['calc'], data['name'])
                init_final_values.append({'n':data['calc_name'], 'c': data['calc']})
        return init_final_values

    def build_init_final_calc(self, calc, set_final_calc=[]):
        for data in calc:
            if data['calc'] != 'sum':
                final_calc = ("if(value.{n}.value != 0){{value.{c}.value = value.{t}.value/value.{n}.value; "
                    "value.{c}.total = value.{t}.value; value.{c}.n = value.{n}.value;}}").format(
                    n=data['n'], c=data['calc_name'], t=data['name'])
                set_final_calc.append(final_calc)
        return set_final_calc               

    def gen_set_emit_keys(self, set_emit_keys):
        return " ".join(set_emit_keys)

    def gen_init_emit_keys(self, init_emit_keys):
        return ", ".join(init_emit_keys)

    def gen_init_values(self, init_values):
        return ", ".join(["{}: {{ value: 0, calc:'sum' }}".format(value) for value in init_values])

    def gen_set_user_values(self, set_user_values):
        return " ".join(set_user_values)

    def gen_set_action_values(self, set_action_values):
        set_action_values_cond = []        
        for action in set_action_values:
            code = " ".join(set_action_values[action].values())    
            cond = "if(z.name == '{}'){{ {} }}".format(action, code)      
            set_action_values_cond.append(cond)
        return " else ".join(set_action_values_cond)

    def gen_adjust_cond_values(self, adjust_cond_values):
        return " ".join(adjust_cond_values)

    def gen_set_bucket_values(self, set_bucket_values):
        return " else ".join(set_bucket_values)

    def gen_set_reduce_sum(self, init_values):
        return " ".join(["result.{v}.value += value.{v}.value;".format(v=value) for value in init_values])
    
    def gen_init_final_values(self, init_final_values):
        return " ".join(["value.{n} = {{ value:0, calc: '{c}', total: 0, n: 0 }};".format(
            **value) for value in init_final_values])
    
    def gen_set_final_calc(self, set_final_calc):    
        return " ".join(set_final_calc)

    def debug(self, mapper, reducer, finalizer):
        print mapper
        print reducer
        print finalizer

    def build_chart(self, db, output, collection, mapper, 
            reducer, finalizer, query):
        db.users.map_reduce(
            Code(mapper), 
            Code(reducer), 
            out={output : collection}, 
            finalize=Code(finalizer), 
            query=query)
