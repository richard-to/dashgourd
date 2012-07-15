from bson.code import Code

def create_cohort_funnel(db, collection, query, group, calc):
    """Creates data for cohort funnels or plain funnels
    
    Creates collection of calculated data for cohort funnels using
    generated map reduce functions.

    Group currently accepts top level user fields, so no events.
        
    Currently can calculate totals, averages percentages.
    
    Args:
        db: PyMongo db instance
        collection: Name of collection to create
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
    """
    
    mapper_template = """ 
    function() {{

        var months = [
            "01", "02", "03", "04", 
            "05", "06", "07", "08", 
            "09", "10", "11", "12"];  
                    
        var values = {{
            count: 1,
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
            count: 0,
            {}
        }}
              
        values.forEach(function(value) {{
            result.count += value.count;
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
            group_init = 'var {m} = this.{m}.getFullYear() + "/" + months[this.{m}.getMonth()] + "/01";'.format(m=value['meta']);
            
        group_init_list.append(group_init)
        group_keys.append("{m}:{m}".format(m=value['meta']))    
   
    for data in calc:
    
        if 'name' not in data:
            if data['type'] == 'avg':
                data['name'] = data['action']
            elif data['type'] == 'pct': 
                data['name'] = "{}_{}".format('has', data['action'])
                
        if 'meta' in data:
            data['name'] = "{}_{}".format(data['name'], data['meta'])
            
        value_list.append(data['name'])
        
        if data['action'] not in value_map_dict:
            value_map_dict[data['action']] = []
        
        if data['type'] == 'avg':
            if 'meta' in data:
                code = "if(z.{m} != undefined){{ values.{n} += z.{m}; }}".format(m=data['meta'], n=data['name'])
            else:
                code = "values.{}++;".format(data['name'])
        elif data['type'] == 'pct':
            code = "values.{} = 1;".format(data['name'])            
        value_map_dict[data['action']].append(code)
    
        if 'calc_name' in data:    
            calc_name = data['calc_name']
        else:
            calc_name = "{}_{}".format(data['type'], data['name'])
        value_final_list.append(calc_name)    
       
        if 'by' not in data:
            data['by'] = 'count'
            
        finalize_calc = "if(value.{by} != 0){{value.{calc_name} = value.{total}/value.{by};}}".format(
            by=data['by'], calc_name=calc_name, total=data['name'])
        value_final_calc.append(finalize_calc)
     
    for key in value_map_dict:
        code = " ".join(value_map_dict[key])    
        cond = "if(z.name == '{}'){{ {} }}".format(key, code)      
        value_map_list.append(cond)
        
    out_group_key = ", ".join(group_keys)
    out_group_init_list = " ".join(group_init_list)
    
    out_values_init = ", ".join(["{}:0".format(value) for value in value_list])
    out_value_map = " else ".join(value_map_list)
    
    out_reduce_sum = " ".join(["result.{v} += value.{v};".format(v=value) for value in value_list])
    
    out_final_values_init = " ".join(["value.{v} = 0;".format(v=value) for value in value_final_list])
    out_final_values_calc = " ".join(value_final_calc)

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
    