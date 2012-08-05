from datetime import datetime
from dashgourd.charts.formatters.helper import FormatHelper

class FormatCombo(object):

    def __init__(self):
        
        self.helper = FormatHelper()
        self.default_formats = self.helper.default_formats

    def build(self, results, fields, group, split=None):
        
        data = []
        data_key = {}
        description = {}
        columns_order = []

        single_field = True if len(fields) == 1 else False
        
        group_name = group['name']
        group_data_type = group.get('data_type', 'date')
        group_label = group.get('label', group['name'].replace('_', ' ').title())

        description[group_name] = (group_data_type, group_label) 
        columns_order = [group_name]

        for result in results:
            
            key = result['_id'][group_name]
            prefix = result['_id'].get(split, None)
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
                    key_value = key             
                    if group_data_type == 'date':
                        key_value = datetime.strptime(key, '%Y/%m/%d')
                    data_key[key] = {'idx': len(data)}
                    row = {group_name : key_value}
                    data.append(row)
                else:
                    row = data[data_key[key]['idx']]
                
                if line_name not in description:
                    columns_order.append(line_name)
                    data_type = field.get('data_type', 'number')
                    description[line_name] = (data_type, label)
                
                row[line_name] = self.helper.format(value[name], field)

        return {
            'data':data, 
            'description':description,
            'columns_order': tuple(columns_order)
        }