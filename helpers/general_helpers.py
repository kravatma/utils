import dateutil.parser as dtparser
import yaml
import json



def dates_range(date_from, date_to, frame='day', output_type='str', output_format='%Y-%m-%d'):
    if type(date_from) is not datetime:
        date_from = dtparser.parse(date_from)
    if type(date_to) is not datetime:
        date_to = dtparser.parse(date_to)
    dates = [date_from + timedelta(n) for n in range(int ((date_to - date_from).days)+1)]
    dates.sort()
    if frame == 'day' and output_type=='str':
        dates = list(map(lambda x: x.strftime(output_format), dates))
    
    return dates
    

def read_yaml(fpath):
    try:
        with open(fpath, 'r') as stream:
            res = yaml.load(stream)
        return res
    except Exception as exc:
        return exc

    
def read_json(filepath):
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except Exception as exc:
        return exc


def write_json(obj, filepath):
    try:
        with open(filepath, 'w') as f:
            json.dump(obj, filepath)
        return 'ok'
    except Exception as exc:
        return exc

        