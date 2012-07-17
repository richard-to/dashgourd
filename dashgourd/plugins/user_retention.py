import math
from bson.code import Code

def create_user_retention(db, collection, options):
    """Creates user retention cohort
    
    Creates user retention cohort charts. 
    
    Date needs to be included first, but can also add other fields 
    to the grouping, such as gender.
    
    Also group currently accepts top level user fields, so no events.
    
    Currently only computes monthly groupings. Weekly is trickier and
    is not implemented yet. May be as simple as using getDay() on date 
    object and subtracting difference from 0 from getDate().
    
    Currently can only specify one action, but it should be 
    possible to specify more in the future.
    
    Args:
        db: PyMongo db instance
        collection: Name of collection to create
        options: Dict with query, group and calc fields        
            query: Mongo db query to select rows to operate on
            group: List of dicts to define how data is grouped
            action: Name of action to track for that month.
    
    Example:
    
    query = {"gender": "Female", "actions": { "$exists": True}}
    
    group = [
        {'meta': 'created_at', 'type': 'monthly'}  
    ]
    
    action = 'signedin'
    
    TODO(richard-to): Error check options more thoroughly
    """

    query = options.get('query')
    group = options.get('group')
    action = options.get('action')
    
    if query is None or group is None or action is None:
        return False
        
    mapper_template = """
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
        
        {init_emit_key}
        var startDateStr = {emit_date_field};   
        var startDate = new Date(startDateStr);
        
        var values = {{}};
        values[startDateStr] = {{count: 1}};
        
        this.actions.forEach(function(z){{
            if(z.name == '{action_name}'){{
                
                {interval_code}
                
                if(z.created_at >= startDate && values[createdDateStr] == undefined){{
                    values[createdDateStr] = {{count: 1}};
                }}
            }}
        }});       
        emit({{ {emit_key} }}, values);        
    }}
    """
    
    reducer_template = """    
    function(key, values){
        var result = {}
              
        values.forEach(function(value){
            for(key in value){
                if(result[key]){
                    result[key].count += value[key].count;
                } else {
                    result[key] = {count: value[key].count};
                }
            }
        });
        return result;
    }
    """
    
    finalizer_template = """
    function(key, value){{
        startDate = key.{};
        var total = value[startDate].count;
        if(total > 0){{
            for(date in value){{
                value[date].pct = value[date].count/total;
            }}
        }}
        return value;
    }}
    """
    
    group_key_date = ''
    group_keys = []
    group_init_list = []
    
    interval = 'monthly'
    interval_code = ''
    
    for value in group:
        
        if 'type' not in value or value['type'] == 'value':
            group_init = 'var {m} = this.{m};'.format(m=value['meta'])
        elif value['type'] == 'monthly':
            group_init = 'var {m} = this.{m}.getFullYear() + "/" + nums[this.{m}.getMonth()] + "/01";'.format(m=value['meta'])
            group_key_date = value['meta']
            interval = value['type']
        elif value['type'] == 'weekly':
            group_init = ("this.{m}.setDate(this.{m}.getDate() - this.{m}.getDay()); " +
                "var {m} = this.{m}.getFullYear() + '/' + nums[this.{m}.getMonth()] + '/' + nums[this.{m}.getDate()-1];").format(m=value['meta'])
            group_key_date = value['meta']
            interval = value['type']
              
        group_init_list.append(group_init)
        group_keys.append("{m}:{m}".format(m=value['meta']))
    
    if interval == 'monthly':
        interval_code = ("var createdDateStr = z.created_at.getFullYear() + '/' + nums[z.created_at.getMonth()] + '/01'; " +
            "z.created_at.setDate(1);");
    elif interval == 'weekly':
        interval_code = ("z.created_at.setDate(z.created_at.getDate() - z.created_at.getDay()); " +
            "var createdDateStr = z.created_at.getFullYear() + '/' + nums[z.created_at.getMonth()] + '/' +  nums[z.created_at.getDate()-1];")
    
    out_group_key = ", ".join(group_keys)
    out_group_init_list = " ".join(group_init_list)
    
    """
    print mapper_template.format(
        emit_key=out_group_key,
        emit_date_field=group_key_date,
        interval_code=interval_code,        
        init_emit_key=out_group_init_list,
        action_name=action)
    
    print reducer_template
    
    print finalizer_template.format(group_key_date)
    """
    
    mapper = Code(mapper_template.format(
        emit_key=out_group_key,
        emit_date_field=group_key_date,
        interval_code=interval_code,
        init_emit_key=out_group_init_list,
        action_name=action))
    reducer = Code(reducer_template)
    finalizer = Code(finalizer_template.format(group_key_date))
    
    db.users.map_reduce(
        mapper, reducer, 
        out={'replace' : collection}, 
        finalize=finalizer, query=query)
    
def format_user_retention(data, dates_max=None, interval_max=None):
    """Formats user retention data for output

    Currently does not support additional groupings
    like date+gender. Mainly seems unreasonable to do.
    
    Better creating separate retention charts for male/female 
    in this case.
    
    This is geared more for output in html. Can also send data 
    as JSON using python json library.
    
    Args:
        data:User retention data returned from monogdb
        dates_max: Max dates to show from newest to oldest
        interval_max: Max date intervals to show.
    
    Return:
        ordered_data: Ordered data with formatted percents and css classes
    """
    
    ordered_data = []
    temp_ordered_data = []
    
    for item in data:
        temp_ordered_data.append(item)
                
        item['output'] = []
        temp_output = []
        
        for key in item['value']:
            output = {
                'date': key,
                'count': item['value'][key]['count'],
                'pct': '{0:.1%}'.format(item['value'][key]['pct']),
                'css_class': 'level_{:02.0f}'.format(math.floor(item['value'][key]['pct'] * 10))
            }           
            temp_output.append(output)
            
        temp_output.sort(key=lambda item:item['date'])
        
        if interval_max is not None:
            item['output'] = temp_output[:interval_max]
        else:
            item['output'] = temp_output
        
    temp_ordered_data.sort(key=lambda item:item['_id']['created_at'])
    if dates_max is not None:
        ordered_data = temp_ordered_data[(dates_max*-1):]
    else:
        ordered_data = temp_ordered_data        

    return ordered_data    
    
    