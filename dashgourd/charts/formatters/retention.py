class FormatRetention(object):
    
    def build(self, results):
        columns_order = ['created_at']
        description = {
            'created_at': ('string', 'Date')
        }
        data = []

        for result in results:
            row = {
                'created_at': result['_id']['created_at']
            }
            date_list = []
            for key in result['value']:        
                date_list.append(key)
            date_list.sort()

            count = 0
            for key in date_list:
                value = result['value'][key]

                if count == 0:
                    col_name = 'total'
                    description[col_name] = ('number', 'Total')
                    row[col_name] = value['count']
                else:
                    col_name = str(count)
                    description[col_name] = ('number', col_name)
                    row[col_name] = (value['pct']['value'], '{:.1%}'.format(value['pct']))
                
                if col_name not in columns_order:
                    columns_order.append(col_name)
                
                count += 1
                
            data.append(row)

        data.sort(key=lambda item:item['created_at'])
        
        return {
            'data':data, 
            'description':description, 
            'columns_order': tuple(columns_order)
        }