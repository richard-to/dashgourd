from bson.code import Code
from datetime import datetime

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
        
        var temp_values = {{}}

        {init_emit_key}

        {attrib_values}

        this.actions.forEach(function(z){{
            {map_values}                       
        }});
        
        {bucket_values}

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
        'exactly': '==',
        'bucket': ''
    }
    
    attrib_list = []

    temp_value_list = []
    value_list = []
    value_map_dict = {}
    value_map_list = []
    value_adjust_list = []
    
    value_final_list = []
    value_final_calc = []
    
    group_keys = []
    group_init_list = []
    
    bucket_cond_list = []  

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
            
            if "attrib" in data:
                action_name = data['action']
            elif type(data['action']) is list:
                action_name = "_or_".join(data['action'])
            else:
                action_name = data['action']
            
            cond = {
                'type': "total",
                'value': 1
            }
            
            if type(data.get('cond')) is dict:
                if data['cond'].get('type') in accepted_conditions:
                    cond['type'] = data['cond']['type']
                
                if  data['cond'].get('value') is not None:
                    cond['value'] = data['cond']['value']

            data['cond'] = cond

            if data['type'] == 'pct': 
                            
                if data['cond']['type'] != 'total' and data['cond']['type'] != 'bucket':               
                    data['name'] = "{}_{}_{}_{}".format(
                        'has', data['cond']['type'], data['cond']['value'], action_name)
                else:
                    data['name'] = action_name
            else:
                data['name'] = action_name
                
        if 'meta' in data:
            data['name'] = "{}_{}".format(data['name'], data['meta'])
        
        if data['name'] not in value_list:    
            value_list.append(data['name'])
                
        if "attrib" in data:
            code = "temp_values.{name} = this.{name}; ".format(name=data['name'])
        elif 'meta' in data:
            code = "if(z.{m} != undefined){{ values.{n}.value += z.{m}; }}".format(m=data['meta'], n=data['name'])
        else:
            code = "values.{}.value++;".format(data['name'])
        
        if "attrib" in data:    
            attrib_list.append(code)
        else:
            if type(data['action']) is list:
                action_list = data['action']
            else:
                action_list = [data['action']]
            
            for action in action_list:       
                if action not in value_map_dict:
                    value_map_dict[action] = {}
                                       
                value_map_dict[action][data['name']] = code
        
        if data['cond']['type'] == 'bucket':
            
            for bucket in data['cond']['value']:

                if type(bucket) is str:
                    
                    if data['name'] not in temp_value_list:    
                        temp_value_list.append(data['name'])

                    value_list.append("{name}_{bucket}".format(name=data['name'], bucket=bucket))
                    bucket_cond = ("if(temp_values.{name} == '{bucket}'){{ " +
                        "values.{name}_{clean_bucket}.value = 1; " + 
                        "}}").format(name=data['name'], bucket=bucket, clean_bucket=bucket)
                elif type(bucket) is int:
                    value_list.append("{name}_{bucket}".format(name=data['name'], bucket=bucket))
                    bucket_cond = ("if(values.{name}.value == {bucket}){{ " +
                        "values.{name}_{bucket}.value = 1; " + 
                        "}}").format(name=data['name'], bucket=bucket)
                elif len(bucket) == 2 and bucket[1] is None:
                    value_list.append("{name}_{min}_plus".format(
                        name=data['name'], min=bucket[0]))
                    bucket_cond = ("if(values.{name}.value >= {min}){{ " +
                        "values.{name}_{min}_plus.value = 1; " + 
                        "}}").format(name=data['name'], min=bucket[0])
                else:
                    value_list.append("{name}_{min}_to_{max}".format(
                        name=data['name'],  min=bucket[0], max=bucket[1]))
                    bucket_cond = ("if(values.{name}.value >= {min} && values.{name}.value <= {max}){{ " +
                        "values.{name}_{min}_to_{max}.value = 1;" + 
                        "}}").format(name=data['name'], min=bucket[0], max=bucket[1])
                bucket_cond_list.append(bucket_cond)

        elif data['type'] == 'pct' and data['cond']['type'] != 'total' and data['cond']['type'] != 'bucket':              
            cond_code = ("values.{name}.value = (values.{name}.value {op} {value}) ? 1 : 0; ").format(
                name=data['name'], op=accepted_conditions[data['cond']['type']], value=data['cond']['value'])
            value_adjust_list.append(cond_code)
        
        if data['type'] != 'total':
            
            if 'by' not in data:
                data['by'] = 'count'
            
            if data['cond']['type'] == 'bucket':
                for bucket in data['cond']['value']:

                    if type(bucket) is str:
                        bucket_name = "{name}_{bucket}".format(name=data['name'], bucket=bucket)
                        calc_name = "{}_{}".format(data['type'], bucket_name)
                        value_final_list.append({'name': calc_name, 'type':data['type']})    
                                       
                        finalize_calc = ("if(value.{by}.value != 0){{value.{calc_name}.value = value.{total}.value/value.{by}.value; " +
                            "value.{calc_name}.total = value.{total}.value; value.{calc_name}.by = value.{by}.value;}}").format(
                            by=data['by'], calc_name=calc_name, total=bucket_name)
                        value_final_calc.append(finalize_calc)            
            else:
                if 'calc_name' in data:
                    calc_name = data['calc_name']
                else:
                    calc_name = "{}_{}".format(data['type'], data['name'])
                value_final_list.append({'name': calc_name, 'type':data['type']})    
                               
                finalize_calc = ("if(value.{by}.value != 0){{value.{calc_name}.value = value.{total}.value/value.{by}.value; " +
                    "value.{calc_name}.total = value.{total}.value; value.{calc_name}.by = value.{by}.value;}}").format(
                    by=data['by'], calc_name=calc_name, total=data['name'])
                value_final_calc.append(finalize_calc)
    
    for value in temp_value_list:
        value_list.remove(value)

    for action in value_map_dict:
        code = " ".join(value_map_dict[action].values())    
        cond = "if(z.name == '{}'){{ {} }}".format(action, code)      
        value_map_list.append(cond)

    out_group_init_list = " ".join(group_init_list)        
    out_group_key = ", ".join(group_keys)

    out_values_init = ", ".join(["{}: {{ value: 0, type:'total' }}".format(value) for value in value_list])
    out_value_map = " else ".join(value_map_list)

    out_values_init = ", ".join(["{}: {{ value: 0, type:'total' }}".format(value) for value in value_list])
    out_attrib_values = " ".join(attrib_list)

    out_values_bucket = " else ".join(bucket_cond_list)

    out_values_adjust = " ".join(value_adjust_list)
    
    out_reduce_sum = " ".join(["result.{v}.value += value.{v}.value;".format(v=value) for value in value_list])
    
    out_final_values_init = " ".join(["value.{name} = {{ value:0, type: '{type}', 'total': 0, 'by': 0 }};".format(
        **value) for value in value_final_list])
    out_final_values_calc = " ".join(value_final_calc)
    
    print mapper_template.format(
        emit_key=out_group_key, 
        init_emit_key=out_group_init_list,
        init_values=out_values_init,
        attrib_values=out_attrib_values,
        map_values=out_value_map,
        bucket_values=out_values_bucket,
        adjust_values=out_values_adjust)

    print reducer_template.format(out_values_init, out_reduce_sum)

    print finalizer_template.format(
        out_final_values_init, out_final_values_calc)

    mapper = Code(mapper_template.format(
        emit_key=out_group_key, 
        init_emit_key=out_group_init_list,
        init_values=out_values_init,
        attrib_values=out_attrib_values,
        map_values=out_value_map,
        bucket_values=out_values_bucket,
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
    
    `data` is list of data in rows
    `description` is a dictionary of all columns
    `columns_order` is a tuple of ordered columns that you want to display.
        
    Args:
        results: Dict returned from pymongo.
        fields: List of dicts that describe which fields to format.
        order: List of dict group keys and order direction. None will use default order.
        max_groups: Amount of data to return. Important for date cohorts mainly.
        
    Returns:
        vis_data: Data formatted for visualisation     
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

def format_abtest(results, title, fields, order=None):
    """Formats data for ab test
    
    Args:
        results: Dict returned from pymongo.
        title: Title of chart
        fields: List of dicts that describe which fields to format.
        order: List of dict group keys and order direction. None will use default order.
        
    Returns:
        dict: title, chart_type, data, description, columns_order   
    """
    
    chart_type = 'ab_table'
    
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
    
    columns_order = ['metric'] 

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
                display = '<span class="subtext">({}/{})</span> {}'.format(
                    int(value[name]['total']),
                    int(value[name]['by']), 
                    data_format.format(value[name]['value']))
                row[variation] = (value[name]['value'], display)

            
            if order is None:
                order = []
                for key in result['_id']:
                    order.append({"name":result['_id'][key]})
    
    control = variations[0]
    for key in data_key:
        idx = data_key[key]['idx']
        calc_type = data_key[key]['calc_type']
        data_format = field.get('format', change_formats['pct'])
        row = data[idx]   
        for variation in variations:
            change_key = "change_{}".format(variation)
            
            if calc_type == 'total':
                if row[control] != 0:
                    change = (row[variation] / float(row[control])) - 1.0
                else:
                    change = 0
            else:
                if row[control][0] != 0:
                    change = (row[variation][0] / row[control][0]) - 1
                else:
                    change = 0
            
            row[change_key] = (change, data_format.format(change))
            
            if variation not in columns_order:
                columns_order.append(variation)
                if variation != control:
                    columns_order.append(change_key)

    return {
        'title': title,
        'chart_type': chart_type,
        'data':data, 
        'description':description,
        'columns_order': tuple(columns_order)
    }
    
    
def format_chart(results, title, fields, x_axis=None, group=None, chart_type="line"):
    """Formats data for line/bar/area charts
    
    Args:
        results: Dict returned from pymongo.
        title: Title of chart
        fields: List of dicts that describe which fields to format.
        order: List of dict group keys and order direction. None will use default order.
        columns_order: Tuple of columns to show
        
    Returns:
        dict: title, chart_type, data, description, columns_order    
    """
    
    default_formats = {
        'total': '{}',
        'avg': '{:.2}',
        'pct': '{:.1%}'
    }
        
    description = {}    
    data = []
    data_key = {}
    description['date'] = ("date", "Date")

    columns_order = ['date']
    
    single_field = True if len(fields) == 1 else False
    
    for result in results:
        
        key = result['_id'][x_axis];
        
        prefix = None
        if group is not None and group in result['_id']:
            prefix = result['_id'][group]
                        
        value = result['value']
        
        for field in fields:
            
            name = field['name']
            
            if prefix is None:
                line_name = name
                label = field.get('label', name.replace('_', ' ').title())
            elif single_field:
                line_name = prefix
                label = line_name.title()
            else:
                line_name = "{}_{}".format(prefix, name)
                label = " ".join([prefix, field.get('label', name.replace('_', ' ').title())])
            
            if key not in data_key:
                dt = datetime.strptime(key, '%Y/%m/%d')
                data_key[key] = {'idx': len(data)}
                row = {'date': dt}
                data.append(row)
            else:
                row = data[data_key[key]['idx']]
            
            if line_name not in description:
                columns_order.append(line_name)
                description[line_name] = ("number", label)
            
            calc_type = value[name]['type']
            data_format = field.get('format', default_formats[calc_type])
            
            if calc_type == 'pct':
                data_value = value[name]['value']*100
                chart_type = "pct_line"
            else:
                data_value = value[name]['value']

            row[line_name] = (
                data_value, data_format.format(value[name]['value']))

    return {
        'title': title,
        'chart_type': chart_type,
        'data':data, 
        'description':description,
        'columns_order': tuple(columns_order)
    }    