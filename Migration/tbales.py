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




mysql_conn = connection.connect(host='localhost', database='mydatabase', user='root', passwd='')
# my=pd.read_sql("select * from xyz;" , mysql_conn)

cursor = mysql_conn.cursor()
query = "SELECT DATA_TYPE,COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_schema = 'mydatabase' and table_name = N'orders' order by column_name"
# query="SELECT *, ISDATE(‘Data_type’) AS “Format” FROM INFORMATION_SCHEMA.COLUMNS where table_name=xorders"
# query="ALTER TABLE orders MODIFY orderDate datetime "
df = pd.io.sql.read_sql(query, mysql_conn)
cursor.execute(query)
recor=cursor.fetchall()
for i in range(len(recor)):
    #converts 'date' datatype to 'datatime'
    if recor[i][0]=='date':
        query="ALTER TABLE orders MODIFY "+recor[i][1]+" datetime "
        cursor.execute(query)

        # print(recor[i][0])

        # print(i)
# print(type(recor))