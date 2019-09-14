from myutils.helpers.general_helpers import read_yaml
from pandas import read_sql, DataFrame
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import pymssql


def easy_engine(creds, db):
    connection_params = read_yaml(creds)[db]
    connection_params['db'] = db
    if db=='mssql':
        connection_params['db'] = 'mssql+pymssql'
    url = '%(db)s://%(user)s:%(password)s@%(address)s:%(port)s/%(dbname)s' % connection_params
    engine = create_engine(url, echo=False)

    return engine
    

def sql_select(engine, table=None, columns=None, where=None, query=None):
    conn = engine.connect()
    if columns is None:
        columns = ['*']
    if where is None:
        where_string=''
    else:
        where_string=' where {}'.format(where)
    
    if table is not None:
        sql_string = 'select {} from {}{}'.format(','.join(columns), table, where_string)
    elif query is not None:
        sql_string = query
    else:
       return 'error: define table or query'
       
    df = read_sql(con=conn, sql=sql_string)
    conn.close()
    return df
    

def sql_insert(engine, df, table, behavior='append'):
    conn = engine.connect()
    try:
        df.to_sql(con=conn, name=table, if_exists=behavior, index=False)
        conn.close()
        print('ok')
        return 
    except IntegrityError:
        conn.close()
        print('fail')
        return 'rows existed'
    except Exception as exc:
        conn.close()
        return exc
        

class mssql_connection():
    def __init__(self, creds):
        con_params = read_yaml(creds)['mssql']
        self.last_query_string = ''
        self.last_query_data = None
        self.state = 'open'
        self.connection = pymssql.connect(server=con_params['address']
                                        , user=con_params['user']
                                        , password=con_params['password'])
#                                        , database=con_params['dbname'])
                                        
    def select(self, table=None, columns=None, where=None, query=None):
        if columns is None:
            columns = ['*']
        if where is None:
            where_string=''
        else:
            where_string=' where {}'.format(where)
        
        if table is not None:
            query_string = 'select {} from {}{}'.format(','.join(columns), table, where_string)
        elif query is not None:
            query_string = query
        else:
           return 'error: define table or query'
        
        cursor = self.connection.cursor()
        cursor.execute(query_string)
        data_rows = cursor.fetchall()
        columns = [row[0] for row in cursor.description]
        
        df = DataFrame(data_rows, columns=columns)
        
        self.last_query_string = query_string
        self.last_query_data = df
        return df
    
    
    def insert(self, data, table, columns=None):
        query="""INSERT INTO %(table)s (%(cols)s) VALUES (%(vals)s)"""
        if isinstance(data, dict):
            columns = list(data.keys())
            values = list(data.values())
        columns_string = ','.join(columns)
        values_string = ','.join(["'%s'" % str(val) for val in values])
        query_string = query %({'table': table, 'cols': columns_string,'vals': values_string})
        
        cursor = self.connection.cursor()
        self.last_query_string = query_string
        try:
            cursor.execute(query_string)
            self.connection.commit()
            return query_string
            
        except Exception as exc:
            return exc
        
        
        

        
        
        self.last_query_string = query_string
        return query_string

    def close(self):
        self.connection.close()
        self.state = 'closed'
    
    
    
    
    
    
    
    