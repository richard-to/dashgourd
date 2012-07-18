from bson.code import Code

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
        {"type":"pct", "action":"bought_song"},
        {"type":"avg", "action":"listened_song", "meta": "time", "by":"listened_song"}    
    ]
    
    TODO(richard-to): Error check options more thoroughly
    TODO(richard-to): Option to print generated functions instead of running them.
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
    
    value_list = []
    value_map_dict = {}
    value_map_list = []
    
    value_final_list = []
    value_final_calc = []
    
    group_keys = []
    group_init_list = []
    
    for value in group:
        
        if 'type' not in value or value['type'] == 'value':
            group_init = 'var {m} = this.{m};'.format(m=value['meta'])
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
                data['name'] = "{}_{}".format('has', action_name)
            else:
                data['name'] = action_name
                
        if 'meta' in data:
            data['name'] = "{}_{}".format(data['name'], data['meta'])
        
        if data['name'] not in value_list:    
            value_list.append(data['name'])
                
        if data['type'] == 'pct':
            code = "values.{}.value = 1;".format(data['name'])
        else:
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
        
    out_group_key = ", ".join(group_keys)
    out_group_init_list = " ".join(group_init_list)
    
    out_values_init = ", ".join(["{}: {{ value: 0, type:'total' }}".format(value) for value in value_list])
    out_value_map = " else ".join(value_map_list)
    
    out_reduce_sum = " ".join(["result.{v}.value += value.{v}.value;".format(v=value) for value in value_list])
    
    out_final_values_init = " ".join(["value.{name} = {{ value:0, type: '{type}', 'total': '{total}', 'by': '{by}' }};".format(
        **value) for value in value_final_list])
    out_final_values_calc = " ".join(value_final_calc)
    
    """
    print mapper_template.format(
        emit_key=out_group_key, 
        init_emit_key=out_group_init_list,
        init_values=out_values_init,
        map_values=out_value_map)
    
    print reducer_template.format(out_values_init, out_reduce_sum)
    
    print finalizer_template.format(
        out_final_values_init, out_final_values_calc)
    """
     
    mapper = Code(mapper_template.format(
        emit_key=out_group_key, 
        init_emit_key=out_group_init_list,
        init_values=out_values_init,
        map_values=out_value_map))
    reducer = Code(reducer_template.format(out_values_init, out_reduce_sum))        
    finalizer = Code(finalizer_template.format(
        out_final_values_init, out_final_values_calc))
    
    db.users.map_reduce(
        mapper, reducer, 
        out={'replace' : collection}, 
        finalize=finalizer, query=query)

def format_cohort_funnel(data, fields, order=None, max_groups=None):
    """Formats data for cohort funnels.
    
    Note that if you have a group with multiple field permutations, max_groups 
    may not be effective as only set of data will be shown. For example if you 
    sorted by gender and then date
    Args:
        data: Dict returned from pymongo.
        fields: List of dicts that describe which fields to format.
        order: List of group keys. None will use default order.
        max_groups: Amount of data to return. Important for date cohorts mainly.
        
    Returns:
        ordered_data: Data ordered and formatted


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
    
    ordered_data = []
    temp_ordered_data = []
    
    for item in data:
        temp_ordered_data.append(item)
        
        item['output'] = []
        temp_output = []
        
        for field in fields:
            name = field['name']
            
            if 'label' not in field:
                label = name.replace('_', ' ').title()
            else:
                label = field['label']
               
            if 'format' not in field:
                format = default_formats[item['value'][name]['type']]
            else:
                format = field['format']
            
            output = {
                'label': label,
                'value': format.format(item['value'][name]['value'])
            }
            item['output'].append(output)
    
            if order is None:
                order = []
                for key in item['_id']:
                    order.append(item['_id'][key])
    
            item['title'] = "-".join([item['_id'][key] for key in order])
            
    for key in reversed(order):        
        temp_ordered_data.sort(key=lambda item:item['_id'][key], reverse=True)
        
    if max_groups is not None:
        ordered_data = temp_ordered_data[0:(max_groups)]
    else:
        ordered_data = temp_ordered_data        

    return ordered_data
