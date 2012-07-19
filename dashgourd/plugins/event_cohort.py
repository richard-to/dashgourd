from bson.code import Code

def create_event_cohort(db, collection, options):
    """Creates data event based cohorts
    
    This is more of a short term solution to handle 
    cohorts based on events.
    
    This is not too useful since grouping by user 
    attributes makes more sense most of the time.
        
    Unfortunately I need this functionality for a few 
    use cases. Will develop this further as needed.
    
    Unfortunately the current format of putting events as
    embedded documents makes this possibly inefficient. 
    Haven't benchmarked or tried the alternative approach 
    of putting events as top level events.
    
    Restrictions:
    
    Event groups can only be grouped by one value for now.
    
    Events that are grouped must be connected by a meta value 
    like an id. This is needed to group these together into 
    an object and then emit these by the event grouping.
    
    Args:
        db: PyMongo db instance
        collection: Name of collection to create
        options: Dict with query, group and calc fields
            query: Mongo db query to select rows to operate on
            focus: Actions to focus on. They must be connected by a meta value
            user_group: List of dicts to define how data is grouped by user (optional)
            event_group: Dict for event meta to be grouped. Singular for now.
            calc: List of dicts to define how to calculate data
            
    Example (Poor example):
    
    query = {"actions": { "$exists": True}}
    
    focus = {
        'actions': ['listened_song', 'bought_song', 'bought_album'], 
        'meta': 'album_id'
    }
     
    user_group = [
        {'meta': 'gender'},  
    ]
    
    event_group = {'action': 'listened_song', meta': 'created_at', 'type': 'monthly'}
    
    calc = [
        {"type":"avg", "action":"listened_song"},
        {"type":"pct", "action":"listened_song"},
        {"type":"avg", "action":"bought_song"},
        {"type":"pct", "action":"bought_song", cond: {type:"at_least", value: 1}},
        {"type":"avg", "action":"listened_song", "meta": "time", "by":"listened_song"}    
    ]
    
    TODO(richard-to): A lot of duplicate code from cohort funnel. Hard to read too.
    """
    
    query = options.get('query')
    focus = options.get('focus')
    user_group = options.get('user_group')
    event_group = options.get('event_group')
    calc = options.get('calc')
    
    if focus is None or query is None or event_group is None or calc is None:
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
        
        var values = {{}}
        
        {init_emit_key}
        
        var emit_key = {{
            {emit_key}
        }}
        
        this.actions.forEach(function(z){{
            
            if({init_values_cond}){{
                if(values[z.{event_meta}] == undefined){{
                    values[z.{event_meta}] = {{
                        'count':{{'value':0, type:'total'}},
                        {init_values}                       
                    }};
                }}
            }}
            
            {map_values}
        
        }});
        
        for(key in events) {{
        
            {adjust_values}
            
            emit_key.{event_meta_group} = values[key].{event_meta_group};
            emit(emit_key, values[key]);
        }}        
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

    event_meta = focus['meta']
    
    event_actions = focus['actions']
    
    event_meta_group = event_group['meta']
    
    event_group_init = None
    
    for value in user_group:
        
        if 'type' not in value or value['type'] == 'value':
            group_init = 'var {m} = this.{m};'.format(m=value['meta'])
        elif value['type'] == 'monthly':
            group_init = 'var {m} = this.{m}.getFullYear() + "/" + nums[this.{m}.getMonth()] + "/01";'.format(m=value['meta'])
        elif value['type'] == 'weekly':
            group_init = ("this.{m}.setDate(this.{m}.getDate() - this.{m}.getDay()); " +
                "var {m} = this.{m}.getFullYear() + '/' + nums[this.{m}.getMonth()] + '/' + nums[this.{m}.getDate()-1];").format(m=value['meta'])
        
        group_init_list.append(group_init)
        group_keys.append("{m}:{m}".format(m=value['meta']))    

    if 'type' not in event_group or event_group['type'] == 'value':
        event_group_init = 'values[{n}].{m} = z.{m}; '.format(n=event_meta, m=value['meta'])
    elif event_group['type'] == 'monthly':
        event_group_init = 'values[{n}].{m} = z.{m}.getFullYear() + "/" + nums[z.{m}.getMonth()] + "/01"; '.format(n=event_meta, m=value['meta'])
    elif event_group['type'] == 'weekly':
        event_group_init = ("z.{m}.setDate(z.{m}.getDate() - z.{m}.getDay()); " +
            "values[{n}].{m} = z.{m}.getFullYear() + '/' + nums[z.{m}.getMonth()] + '/' + nums[z.{m}.getDate()-1]; ").format(n=event_meta, m=value['meta'])
    
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
            code = "if(z.{m} != undefined){{ values['e'].{n}.value += z.{m}; }}".format(
                m=data['meta'], e=event_meta, n=data['name'])
        else:
            code = "values['{}'].{}.value++;".format(event_meta, data['name'])
        
            
        if type(data['action']) is list:
            action_list = data['action']
        else:
            action_list = [data['action']]
        
        for action in action_list:       
            if action not in value_map_dict:
                value_map_dict[action] = {}
                                   
            value_map_dict[action][data['name']] = code
        
        if data['type'] == 'pct':                
            cond_code = ("values[key].{name}.value = (values[key].{name}.value {op} {value}) ? 1 : 0; ").format(
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
    
    code = " ".join(["events[{}].count.value = 1; ".format(event_meta), event_group_init])
    cond = "if(z.name == '{}'){{ {} }}".format(event_group['action'], code)
    value_map_list.append(cond)
    
    for action in value_map_dict:
        code = " ".join(value_map_dict[action].values())    
        cond = "if(z.name == '{}'){{ {} }}".format(action, code)      
        value_map_list.append(cond)
                         
    out_group_init_list = " ".join(group_init_list)
    out_group_key = ", ".join(group_keys)
      
    out_init_values_cond = " || ".join(["z.name == '{}'".format(action) for action in event_actions])
    out_event_meta = event_meta
    out_event_meta_group = event_meta_group

    out_values_init = ", ".join(["{}: {{ value: 0, type:'total' }}".format(value) for value in value_list])
    out_value_map = " else ".join(value_map_list)
    
    out_values_adjust = " ".join(value_adjust_list)
    
    out_reduce_sum = " ".join(["result.{v}.value += value.{v}.value;".format(v=value) for value in value_list])
    
    out_final_values_init = " ".join(["value.{name} = {{ value:0, type: '{type}', 'total': '{total}', 'by': '{by}' }};".format(
        **value) for value in value_final_list])
    out_final_values_calc = " ".join(value_final_calc)
                        
    print mapper_template.format(
        emit_key=out_group_key, 
        init_emit_key=out_group_init_list,
        init_values_cond=out_init_values_cond,
        event_meta=out_event_meta,
        adjust_values=out_values_adjust, 
        init_values=out_values_init,
        event_meta_group=out_event_meta_group,
        map_values=out_value_map)
        
    mapper = Code(mapper_template.format(
        emit_key=out_group_key, 
        init_emit_key=out_group_init_list,
        init_values_cond=out_init_values_cond,
        event_meta=out_event_meta,
        adjust_values=out_values_adjust, 
        init_values=out_values_init,
        event_meta_group=out_event_meta_group,        
        map_values=out_value_map))
    reducer = Code(reducer_template.format(out_values_init, out_reduce_sum))        
    finalizer = Code(finalizer_template.format(
        out_final_values_init, out_final_values_calc))
    
    #db.users.map_reduce(
    #    mapper, reducer, 
    #    out={'replace' : collection}, 
    #    finalize=finalizer, query=query)    