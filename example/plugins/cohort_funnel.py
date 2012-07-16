from dashgourd.api.helper import HelperApi

collection = 'cohort_funnel'

query = {"gender": "Male", "actions": { "$exists": True}}

group = [
    {'meta': 'created_at', 'type': 'monthly'},
]

calc = [
    {"type":"avg", "action":"listened_song"},
    {"type":"pct", "action":"listened_song"},
    {"type":"avg", "action":["bought_song", "purchased_song"]},
    {"type":"pct", "action":"bought_song"},
    {"type":"avg", "action":"listened_song", "meta": "time", "by":"listened_song"}    
] 

helper_api = HelperApi()
chart_api = helper_api.get_api('charts')
chart_api.generate_chart('cohort_funnel', collection, 
    {'query':query, 'group':group, 'calc':calc})



"""
    //Generated Mapper Function
    function() {

        var months = [
            "01", "02", "03", "04", 
            "05", "06", "07", "08", 
            "09", "10", "11", "12"];  
                    
        var values = {
            count: 1,
            listened_song:0, has_listened_song:0, bought_song_or_purchased_song:0, has_bought_song:0, listened_song_time:0
        }
        
        var created_at = this.created_at.getFullYear() + "/" + months[this.created_at.getMonth()] + "/01";

        this.actions.forEach(function(z){
            if(z.name == 'purchased_song'){ values.bought_song_or_purchased_song++; } else if(z.name == 'bought_song'){ values.bought_song_or_purchased_song++; values.has_bought_song = 1; } else if(z.name == 'listened_song'){ values.listened_song++; values.has_listened_song = 1; if(z.time != undefined){ values.listened_song_time += z.time; } }                       
        });
        emit({ created_at:created_at }, values);        
    }
    
    //Generated Reducer Function    
    function(key, values){
        var result = {
            count: 0,
            listened_song:0, has_listened_song:0, bought_song_or_purchased_song:0, has_bought_song:0, listened_song_time:0
        }
              
        values.forEach(function(value) {
            result.count += value.count;
            result.listened_song += value.listened_song; result.has_listened_song += value.has_listened_song; result.bought_song_or_purchased_song += value.bought_song_or_purchased_song; result.has_bought_song += value.has_bought_song; result.listened_song_time += value.listened_song_time;
        });

        return result;
    }
    
    //Generated Finalizer Function
    function(key, value){

        value.avg_listened_song = 0; value.pct_has_listened_song = 0; value.avg_bought_song_or_purchased_song = 0; value.pct_has_bought_song = 0; value.avg_listened_song_time = 0;

        if(value.count != 0){value.avg_listened_song = value.listened_song/value.count;} if(value.count != 0){value.pct_has_listened_song = value.has_listened_song/value.count;} if(value.count != 0){value.avg_bought_song_or_purchased_song = value.bought_song_or_purchased_song/value.count;} if(value.count != 0){value.pct_has_bought_song = value.has_bought_song/value.count;} if(value.listened_song != 0){value.avg_listened_song_time = value.listened_song_time/value.listened_song;}
        
        return value;
    }
"""