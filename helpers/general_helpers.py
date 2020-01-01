import dateutil.parser as dtparser
import yaml
import json
import pandas as pd
from datetime import datetime, timedelta
#import datetime
from datetime_truncate import truncate


def dates_range(date_from, date_to, frame='day', output_type='str', output_format='%Y-%m-%d'):
    if not isinstance(date_from, datetime):
        date_from = dtparser.parse(date_from)
    if not isinstance(date_to, datetime):
        date_to = dtparser.parse(date_to)
    dates = [date_from + timedelta(n) for n in range(int((date_to - date_from).days)+1)]
    dates = [truncate(dt, frame) for dt in dates]
    dates = list(set(dates))
    dates.sort()
    if output_type=='str':
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

		
def multiple_merge(dfs, cols, hows):
    main_df = dfs[0].copy()
    left_col = cols[0]
    hows=[''] + hows
    for i in range(1, len(dfs)):
        right_col = cols[i]
        if right_col==left_col:
            main_df = pd.merge(left=main_df, right=dfs[i], on=left_col, how=hows[i])
        else:
            main_df = pd.merge(left=main_df, right=dfs[i], left_on=left_col, right_on=right_col, how=hows[i])
        
    return main_df
