import pandas as pd
import psycopg2 as pg
import sqlite3
from sqlalchemy import create_engine
import pymysql
import mysql.connector as connection
from pandas.io import sql
import psycopg2 as pg
import pandas as pd
import psycopg2 as pg
import pandas as pd
# import lib.configs as configs


mysql_conn = connection.connect(host='localhost', database='nb3', user='root', passwd='')
my=pd.read_sql("select * from xyz;",mysql_conn)
print(my)
dy=my

# mycursor = mysql_conn.cursor()
# mycursor.execute("select * from xyz;")

psql_conn = pg.connect("dbname='Try_psql' user='postgres' host='localhost' port='5434' password='Psqdarsh1@'")

# psql_cursor=psql_conn.cursor()
# engine= create_engine(psql_conn)
# for row in mycursor:
# for data in my:
    # engine= create_engine(psql_conn)

password='Psqdarsh1@'
engine = create_engine('postgresql+psycopg2://postgres:'+password+'@localhost:5434/Try_psql',pool_pre_ping="True")
df=dy.to_sql('pq', engine)
# df=my.to_sql('p', con =psql_conn,if_exists='append')
print(df)






def get_engine(host, port, username, password, db_name="postgres"):
    from sqlalchemy import create_engine
    engine = create_engine(
        "postgresql://" + username + ":" + password + "@" + host + ":" + str(port) + "/" + db_name)
    return engine



def import_from(host, port, username, password, query, db_name, table):
    connection = pg.connect(
        "host=" + host + " dbname=" + db_name + " user=" + username + " password=" + password)
    try:
        df = pd.io.sql.read_sql(query, connection)
        return df
    except:
        return "No Data"



def export_to(host, port, username, password, df, table, db_name):
    df.to_sql(table, get_engine(host, port, username, password, db_name), if_exists='append', index=False)