from datetime import datetime
from dashgourd.charts.formatters.helper import FormatHelper

class FormatTable(object):
    
    def __init__(self):
        
        self.helper = FormatHelper(True)
        self.default_formats = self.helper.default_formats

    def build(self, results, fields, row_order=None):

        data = []        
        description = {}    
        columns_order = []
        
        for field in fields:
            label = field.get('label', field['name'].replace('_', ' ').title())
            data_type = field.get('data_type', 'number')
            description[field['name']] = (data_type, label)
        
        for field in fields:
            columns_order.append(field['name'])

        if row_order is None:
            row_order = []
            for field in fields:
                name = field['name']                
                is_key_col = field.get('is_key_col', False)
                if is_key_col:
                     row_order.append({'name': name})           
        
        for result in results:

            row = {}
            value = result['value']

            for field in fields:
                name = field['name']                
                is_key_col = field.get('is_key_col', False)

                if is_key_col:
                     data_type = field.get('data_type', 'number')
                     key_value = result['_id'][name]
                     
                     if data_type == 'date':
                        key_value = datetime.strptime(key_value, '%Y/%m/%d')
                     row[name] = key_value
                else:                
                    row[name] = self.helper.format(value[name], field)

            data.append(row)

        for col in reversed(row_order):
            data.sort(key=lambda item:item[col['name']], reverse=col.get('reverse', False))
        
        return {
            'data':data, 
            'description':description,
            'columns_order': tuple(columns_order)
        }