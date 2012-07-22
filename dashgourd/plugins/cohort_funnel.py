from bson.code import Code
import gviz_api

def create_cohort_funnel(db, collection, options):
    """Creates data for cohort funnels or plain funnels
    
    Creates collection of calculated data for cohort funnels using
    generated map reduce functions.

    Group currently accepts top level user fields, so no events.
        
    Currently can calculate totals, averages percentages.
    
    Args:
        db: PyMongo db instance
        collection: Name of collection to create
        options: Dict with query, group and calc fields
            query: Mongo db query to select rows to operate on
            group: List of dicts to define how data is grouped
            calc: List of dicts to define how to calculate data
    
    Example:
    
    query = {"gender": "Male", "actions": { "$exists": True}}
    
    group = [
        {'meta': 'created_at', 'type': 'monthly'}  
    ]
    
    calc = [
        {"type":"avg", "action":"listened_song"},
        {"type":"pct", "action":"listened_song"},
        {"type":"avg", "action":"bought_song"},
        {"type":"pct", "action":"bought_song", cond: {type:"at_least", value: 1}},
        {"type":"avg", "action":"listened_song", "meta": "time", "by":"listened_song"}    
    ]
    
    TODO(richard-to): Error check options more thoroughly
    TODO(richard-to): Option to print generated functions instead of running them.
    TODO(richard-to): Add option to do merge, reduce or replace for map reduce.
    TODO(richard-to): Factor out default count value? Or is it always required? Else rename to "total"?
    """
    
    query = options.get('query')
    group = options.get('group')
    calc = options.get('calc')
    
    if query is None or group is None or calc is None:
        return False
    
    mapper_template = """ 
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
            count: {{value:1, type:'total'}},
            {init_values}
        }}
        
        {init_emit_key}

        this.actions.forEach(function(z){{
            {map_values}                       
        }});
        
        {adjust_values}
        
        emit({{ {emit_key} }}, values);        
    }}
    """
    
    reducer_template = """    
    function(key, values){{
        var result = {{
            count: {{value:0, type:'total'}},
            {}
        }}
              
        values.forEach(function(value) {{
            result.count.value += value.count.value;
            {}
        }});

        return result;
    }}
    """
    
    finalizer_template = """
    function(key, value){{

        {}

        {}
        
        return value;
    }}
    """

    accepted_conditions = {
        'at_least': '>=',
        'at_most': '<=',
        'exactly': '=='
    }
            
    value_list = []
    value_map_dict = {}
    value_map_list = []
    value_adjust_list = []
    
    value_final_list = []
    value_final_calc = []
    
    group_keys = []
    group_init_list = []
    
    for value in group:
        
        if 'type' not in value or value['type'] == 'value':
            group_init = 'var {m} = this.{m};'.format(m=value['meta'])
        elif value['type'] == 'ab':
            group_init = 'var {m} = "variation_" + parseInt(this.ab.{m});'.format(m=value['meta'])                        
        elif value['type'] == 'monthly':
            group_init = 'var {m} = this.{m}.getFullYear() + "/" + nums[this.{m}.getMonth()] + "/01";'.format(m=value['meta'])
        elif value['type'] == 'weekly':
            group_init = ("this.{m}.setDate(this.{m}.getDate() - this.{m}.getDay()); " +
                "var {m} = this.{m}.getFullYear() + '/' + nums[this.{m}.getMonth()] + '/' + nums[this.{m}.getDate()-1];").format(m=value['meta'])
                        
        group_init_list.append(group_init)
        group_keys.append("{m}:{m}".format(m=value['meta']))    
   
    for data in calc:
    
        if 'name' not in data:
            
            if type(data['action']) is list:
                action_name = "_or_".join(data['action'])
            else:
                action_name = data['action']
            
            if data['type'] == 'pct': 
                
                cond = {
                    'type': "at_least",
                    'value': 1
                }
                
                if type(data.get('cond')) is dict:
                    if data['cond'].get('type') in accepted_conditions:
                        cond['type'] = data['cond']['type']
                    
                    if  data['cond'].get('value') is not None:
                        cond['value'] = data['cond']['value']
                
                data['cond'] = cond
                
                data['name'] = "{}_{}_{}_{}".format(
                    'has', data['cond']['type'], data['cond']['value'], action_name)
            else:
                data['name'] = action_name
                
        if 'meta' in data:
            data['name'] = "{}_{}".format(data['name'], data['meta'])
        
        if data['name'] not in value_list:    
            value_list.append(data['name'])
                

        if 'meta' in data:
            code = "if(z.{m} != undefined){{ values.{n}.value += z.{m}; }}".format(m=data['meta'], n=data['name'])
        else:
            code = "values.{}.value++;".format(data['name'])
        
            
        if type(data['action']) is list:
            action_list = data['action']
        else:
            action_list = [data['action']]
        
        for action in action_list:       
            if action not in value_map_dict:
                value_map_dict[action] = {}
                                   
            value_map_dict[action][data['name']] = code
        
        if data['type'] == 'pct':                
            cond_code = ("values.{name}.value = (values.{name}.value {op} {value}) ? 1 : 0; ").format(
                name=data['name'], op=accepted_conditions[data['cond']['type']], value=data['cond']['value'])
            value_adjust_list.append(cond_code)
        
        if data['type'] != 'total':
            
            if 'by' not in data:
                data['by'] = 'count'
                            
            if 'calc_name' in data:
                calc_name = data['calc_name']
            else:
                calc_name = "{}_{}".format(data['type'], data['name'])
            value_final_list.append({'name': calc_name, 'type':data['type'], 'total':data['name'], 'by':data['by']})    
                           
            finalize_calc = "if(value.{total}.value != 0){{value.{calc_name}.value = value.{total}.value/value.{by}.value;}}".format(
                by=data['by'], calc_name=calc_name, total=data['name'])
            value_final_calc.append(finalize_calc)
     
    for action in value_map_dict:
        code = " ".join(value_map_dict[action].values())    
        cond = "if(z.name == '{}'){{ {} }}".format(action, code)      
        value_map_list.append(cond)

    out_group_init_list = " ".join(group_init_list)        
    out_group_key = ", ".join(group_keys)

    out_values_init = ", ".join(["{}: {{ value: 0, type:'total' }}".format(value) for value in value_list])
    out_value_map = " else ".join(value_map_list)
    
    out_values_adjust = " ".join(value_adjust_list)
    
    out_reduce_sum = " ".join(["result.{v}.value += value.{v}.value;".format(v=value) for value in value_list])
    
    out_final_values_init = " ".join(["value.{name} = {{ value:0, type: '{type}', 'total': '{total}', 'by': '{by}' }};".format(
        **value) for value in value_final_list])
    out_final_values_calc = " ".join(value_final_calc)
        
    mapper = Code(mapper_template.format(
        emit_key=out_group_key, 
        init_emit_key=out_group_init_list,
        init_values=out_values_init,
        map_values=out_value_map,
        adjust_values=out_values_adjust))
    reducer = Code(reducer_template.format(out_values_init, out_reduce_sum))        
    finalizer = Code(finalizer_template.format(
        out_final_values_init, out_final_values_calc))
    
    db.users.map_reduce(
        mapper, reducer, 
        out={'replace' : collection}, 
        finalize=finalizer, query=query)

def format_cohort_funnel(results, fields, order=None, max_groups=None):
    """Formats data for cohort funnels.
    
    Note that if you have a group with multiple field permutations, max_groups 
    may not be effective as only set of data will be shown. For example if you 
    sorted by gender and then date.
    
    Data is formatted using the Google Visualization Api python library
    
    Args:
        results: Dict returned from pymongo.
        fields: List of dicts that describe which fields to format.
        order: List of dict group keys and order direction. None will use default order.
        max_groups: Amount of data to return. Important for date cohorts mainly.
        
    Returns:
        vis_data: Data formatted for visualisation


    Example:
    
        fields = [
            {'name': 'count', 'label': 'Total Users'},
            {'name': 'avg_purchased_cds'},
            {'name': 'pct_has_purchased_cds'}      
        ]
        
        or explicitly setting format:
        
        fields = [
            {'name': 'count', 'format': '{}', 'label': 'Total Users'},
            {'name': 'avg_purchased_cds', 'format': '{:.2}', 'label': 'Avg Purchased Cds'},
            {'name': 'pct_has_purchased_cds', 'format': '{:.1%}'}      
        ]        
    """ 
    
    default_formats = {
        'total': '{}',
        'avg': '{:.2}',
        'pct': '{:.1%}'
    }
    
    description = {}    
    data = []
   
    for result in results:

        row = {}
        value = result['value']
        for field in fields:
            name = field['name']
            calc_type = value[name]['type']
            label = field.get('label', name.replace('_', ' ').title())
            data_format = field.get('format', default_formats[calc_type])
            
            if calc_type == 'total':
                row[name] = int(value[name]['value'])
            else:
                row[name] = (value[name]['value'], data_format.format(value[name]['value']))
            
            description[name] = ("number", label) 
            
            for key in result['_id']:
                row[key] = result['_id'][key]
                
            if order is None:
                order = []
                for key in result['_id']:
                    order.append({"name":result['_id'][key]})
            
        data.append(row)
        
    for group in reversed(order):
        description[group['name']] = ("string", group.get('label', group['name'].replace('_', ' ').title())) 
        data.sort(key=lambda item:item[group['name']], reverse=group.get('reverse', False))
        
    if max_groups is not None:
        data = data[0:(max_groups)]
    
    return {'data':data, 'description':description}

def format_abtest(results, fields, order=None, max_groups=None):
    """Formats data for ab test
    
    Wrapper for `format_cohort_funnel` but provides net change
    calculation from control.
    
    Args:
        results: Dict returned from pymongo.
        fields: List of dicts that describe which fields to format.
        order: List of dict group keys and order direction. None will use default order.
        max_groups: Amount of data to return. Important for date cohorts mainly.
        
    Returns:
        data: Dict of data list and description of table schema    
    """

    change_formats = {
        'total': '{:+}',
        'avg': '{:+.2}',
        'pct': '{:+.1%}'
    }
        
    default_formats = {
        'total': '{}',
        'avg': '{:.2}',
        'pct': '{:.1%}'
    }
    
    description = {}    
    data = []
    data_key = {}
    variations = []
    description['metric'] = ("string", "Metric")
    description['change'] = ("string", "% Change")

    for result in results:
        
        for key in result['_id']:
            variation = result['_id'][key]
            if variation not in description:
                variations.append(variation)
                description[variation] = ('number', variation.replace('_', ' ').title())
                change_key = "change_{}".format(variation)
                description[change_key] = ("number", "% Change")
                        
        value = result['value']
        for field in fields:
            
            name = field['name']
            
            if name not in data_key:
                data_key[name] = {'idx': len(data), 'calc_type': value[name]['type']}
                row = {'metric': field.get('label', name.replace('_', ' ').title())}
                data.append(row)
            else:
                row = data[data_key[name]['idx']]

            calc_type = value[name]['type']
            data_format = field.get('format', default_formats[calc_type])
            

            if calc_type == 'total':
                row[variation] = int(value[name]['value'])
            else:
                row[variation] = (value[name]['value'], data_format.format(value[name]['value']))

            
            if order is None:
                order = []
                for key in result['_id']:
                    order.append({"name":result['_id'][key]})
    
    control = variations[0]
    for key in data_key:
        idx = data_key[key]['idx']
        calc_type = data_key[key]['calc_type']
        data_format = field.get('format', change_formats[calc_type])
        row = data[idx]   
        for variation in variations:
            change_key = "change_{}".format(variation)
            
            if calc_type == 'total':
                row[change_key] = row[variation] - row[control]
            else:
                change = row[variation][0] - row[control][0]
                row[change_key] = (change, data_format.format(change))

    return {'data':data, 'description':description}