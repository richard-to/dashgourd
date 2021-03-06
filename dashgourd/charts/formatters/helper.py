
class FormatHelper(object):

    def __init__(self, allow_html=False):

        self.allow_html = allow_html
        self.change_formats = {
            'sum': '{:+}',
            'avg': '{:+.2}',
            'pct': '{:+.1%}'
        }
            
        self.default_formats = {
            'sum': '{}',
            'avg': '{:.2}',
            'pct': '{:.1%}'
        }

    def format(self, meta, field):
        calc_type = meta.get('calc', 'sum')
        data_format = field.get('format', self.default_formats[calc_type])

        if calc_type == 'sum':
            formatted_val = self.format_sum(meta, data_format)
        elif calc_type == 'avg':
            formatted_val = self.format_avg(meta, data_format)                  
        elif calc_type == 'pct':
            formatted_val = self.format_pct(meta, data_format)
        return formatted_val

    def format_sum(self, meta, data_format):
        value = meta['value']
        return (int(value), data_format.format(value))

    def format_avg(self, meta, data_format):
        display = self.show_calc(
            meta['value'], meta['total'], meta['n'], data_format)
        return (meta['value'], display)
    
    def format_pct(self, meta, data_format):
        data_value = meta['value']*100
        display = self.show_calc(
            meta['value'], meta['total'], meta['n'], data_format)
        return (data_value, display)
        
    def show_calc(self, value, total, n, data_format):
        display = '({}/{}) {}'    
        if self.allow_html:
            display = '<span class="subtext">({}/{})</span> {}'
        return display.format(
            int(total), int(n), data_format.format(value))