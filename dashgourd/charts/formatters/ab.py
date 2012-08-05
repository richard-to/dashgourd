from dashgourd.charts.formatters import FormatHelper

class FormatAb(object):

    def __init__(self):
        
        self.helper = FormatHelper()
        self.change_formats = self.helper.change_formats
        self.default_formats = self.helper.default_formats

    def build(self, results, fields)

        data = []
        data_key = {}
        description = {}
        columns_order []
        
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
                meta = value[name]

                if name not in data_key:
                    data_key[name] = {'idx': len(data), 'calc_type': meta['calc']}
                    row = {'metric': field.get('label', name.replace('_', ' ').title())}
                    data.append(row)
                else:
                    row = data[data_key[name]['idx']]

                row[variation] = self.helper.format(value[name], field)

        control = variations[0]
        for key in data_key:
            idx = data_key[key['idx']]
            calc_type = data_key[key]['calc_type']
            data_format = field.get('format', change_formats['pct'])
            
            row = data[idx]
            for variation in variations:
            change_key = "change_{}".format(variation)
            
            if calc_type == 'sum':
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
            'data':data, 
            'description':description,
            'columns_order': tuple(columns_order)
        }