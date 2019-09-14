from datetime import datetime, timedelta
from requests import request
from re import findall
from yaml import load
from pandas import read_sql
import dateutil.parser as dtparser
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine
from pandas import DataFrame
from time import sleep
from pymongo import MongoClient
from browsermobproxy import Server
from selenium import webdriver


def dates_range(date_from, date_to, frame='day', delimiter=''):
    if type(date_from) is not datetime:
        date_from = dtparser.parse(date_from)
    if type(date_to) is not datetime:
        date_to = dtparser.parse(date_to)
    dates = [date_from + timedelta(n) for n in range(int ((date_to - date_from).days)+1)]
    if frame == 'day':
        dates = list(set(list(map(lambda x: x.strftime('%Y{}%m{}%d'.format(delimiter, delimiter)), dates))))
    elif frame == 'month':
        dates = list(set(list(map(lambda x: x.strftime('%Y{}%m'.format(delimiter)), dates))))
    dates.sort()
    return dates


def whoscored_matches_raw(stage, date, cookies, headers, delay=1):
    url = "https://www.whoscored.com/tournamentsfeed/{}/Fixtures/?d={}&isAggregate=false".format(str(stage), date)
    response = request("GET", url, headers=headers, cookies=cookies)
    s = response.text
    matches = findall(r"\[{1,2}(.*)\]\r\n", s)
    output = list(map(lambda x: x.split(','), matches))
    sleep(delay)
    return output


def read_yaml(address):
    with open(address, 'r') as stream:
        res = load(stream)
    return res


def sql_select(engine, table, columns=None, where=None):
    conn = engine.connect()
    if columns is None:
        columns = ['*']
    if where is None:
        where_string=''
    else:
        where_string=' where {}'.format(where)
    
    sql_string = 'select {} from {}{}'.format(','.join(columns), table, where_string)
    df = read_sql(con=conn, sql=sql_string)
    conn.close()
    return df


def sql_upsert(engine, df, table):
    conn = engine.connect()
    try:
        df.to_sql(con=conn, name=table, if_exists='append', index=False)
        conn.close()
        print('ok')
        return 'ok'
    except IntegrityError:
        conn.close()
        print('fail')
        return 'rows existed'


def easy_engine(db):
    connection_params = read_yaml('creds.yaml')[db]
    connection_params['db'] = db
    if db != 'mongodb':
        url = '%(db)s://%(user)s:%(password)s@%(address)s:%(port)s/%(dbname)s' % connection_params
        engine = create_engine(url, echo=False)
    else:
        url = '%(db)s://%(user)s:%(password)s@%(address)s:%(port)s/%(dbname)s' % connection_params
        engine = MongoClient(url)

    return engine


def parse_score(ft_score, fh_score):
    if ft_score == "'vs'":
        return [None]*6
    try:
        ft_score = ft_score.strip("'")
        fh_score = fh_score.strip("'")

        hs, gs = [int(i) for i in ft_score.split(' : ')]
        hfh, gfh = [int(i) for i in fh_score.split(' : ')]
        hsh = hs-hfh
        gsh = gs-gfh
        return hs, gs, hfh, gfh, hsh, gsh
    except:
        print(ft_score, fh_score)
        return [None]*6


def beautify_matches(matches_list):
    columns = ['match_id', 'tournament_id', 'weekday', 'date', 'time', 'home_id', 'home_name', 'home_redcard',
        'guest_id', 'guest_name', 'guest_redcard', 'fulltime_score', 'fh_score', 'w1', 'w2', 'ftbool', 'w3', 'w4',
        'w5', 'w6', 'w7', 'season', 'champ', 'country1', 'country2', 'country3']
    df = DataFrame(matches_list, columns=columns)
    #df.to_csv('L:/backuprawdf.csv', sep=';', index=False)
    df[['date']] = df[['date']].applymap(dtparser.parse)
    beautydf = df[['match_id', 'champ', 'date', 'time', 'season', 'home_id', 'guest_id']].copy()
    beautydf.rename(columns={'match_id': 'id'}, inplace=True)
    l1 = list(df['fulltime_score'])
    l2 = list(df['fh_score'])
    beautydf[['hs', 'gs', 'hfh', 'gfh', 'hsh', 'gsh']] = DataFrame(list(map(lambda x, y: parse_score(x, y), l1, l2)))
    beautydf.drop_duplicates(inplace=True)
    beautydf[['champ']] = beautydf[['champ']].applymap(lambda x: x.strip("'\""))
    beautydf[['time']] = beautydf[['time']].applymap(lambda x: x.strip("'\""))
    beautydf[['season']] = beautydf[['season']].applymap(lambda x: x.strip("'\""))
    return beautydf


def easy_browser(browser='Chrome', proxywebmob=True, headless=False):
    server = Server("D:/Programms/browsermob-proxy/bin/browsermob-proxy")
    server.start()
    proxy = server.create_proxy()
    if browser == 'Chrome':
        chrome_options = webdriver.ChromeOptions()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--proxy-server={0}".format(proxy.proxy))
        driver = webdriver.Chrome(chrome_options=chrome_options)
    return driver, server, proxy
