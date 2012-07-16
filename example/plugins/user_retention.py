from dashgourd.api.helper import HelperApi

collection = 'user_retention'
query = {"actions": { "$exists": True}}
group = [
    {'meta': 'created_at', 'type': 'monthly'}  
]
action = 'signedin'

helper_api = HelperApi()
chart_api = helper_api.get_api('charts')
chart_api.generate_chart('user_retention', collection, 
    {'query':query, 'group':group, 'action':action})

"""
    //Generated Mapper Function
    function(){

        var months = [
            "01", "02", "03", "04", 
            "05", "06", "07", "08", 
            "09", "10", "11", "12"];  
        
        var created_at = this.created_at.getFullYear() + "/" + months[this.created_at.getMonth()] + "/01";
        var startDateStr = created_at;   
        var startDate = new Date(startDateStr);
        
        var values = {};
        values[startDateStr] = {count: 1};
        
        this.actions.forEach(function(z){
            if(z.name == 'signedin'){
                var createdDateStr = z.created_at.getFullYear() + "/" + months[z.created_at.getMonth()] + "/01";
                z.created_at.setDate(1);
                if(z.created_at >= startDate && values[createdDateStr] == undefined){
                    values[createdDateStr] = {count: 1};
                }
            }
        });       
        emit({ created_at:created_at }, values);        
    }
    
    //Generated Reducer Function
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
    
    //Generated Finalizer Function
    function(key, value){
        startDate = key.created_at;
        var total = value[startDate].count;
        if(total > 0){
            for(date in value){
                value[date].pct = value[date].count/total;
            }
        }
        return value;
    }
"""
