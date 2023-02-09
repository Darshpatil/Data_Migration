import pandas as pd
import psycopg2 as pg
import sqlite3
from sqlalchemy import *
import pymysql
import mysql.connector as connection
from pandas.io import sql


#INPUT FROM USERS
# host = str(input("host name\n"))
#
# database = str(input("enter db name\n"))
# user = str(input("enter user name\n"))
#
# passwd = str(input("enter password\n"))

option = int(input("1: Own Query \n2: Defined Query\n"))


#IF USERS WANT CUSTOMIZED QUERIES
if option == 2:


    conn = connection.connect(host='localhost', database='nb3', user='root', passwd='')
    mycursor = conn.cursor()
    mycursor.execute("Show tables;")
    myresult = mycursor.fetchall()
    i=0

    print("\n\nSELECT THE TABLE TO MIGRATE\n")
    for tables in myresult:
        i=i+1
        print(i,tables)
        continue

    tb=int(input("enter the table number\n"))
    tn=1
    for tables in myresult:

        if tn==tb:

            # conn = connection.connect(host='localhost', database='nb3', user='root', passwd='')

            #stores the selected table
            str = ''
            for item in tables:
                str = str+item
                a=str
                print(a)
                df = pd.read_sql("select * from " +a+ ";", conn)
                # print(df)


            #to migrate to new database
            new_db=(input("enter the db name to migrate the table\n"))
            url = "mysql+pymysql://root:""@127.0.0.1/"+new_db+""
            engine = create_engine(url)



            # New name of the table
            New_tb=input("enter the new table name\n")
            df.to_sql(name=New_tb, con=engine, if_exists='replace')
            break


        else:
            #increment the value to match with tb value
            tn=tn+1
            continue







else:
    op1 = str(input("Defined your query\n"))
    conn = connection.connect(host='localhost', database='nb3', user='root', passwd='')

    # to migrate to new database
    df = pd.read_sql(""+op1+"", conn)
    new_db = (input("enter the db name to migrate the table\n"))
    url = "mysql+pymysql://root:""@127.0.0.1/" + new_db + ""
    engine = create_engine(url)

    # New name of the table
    New_tb = input("enter the new table name\n")
    df.to_sql(name=New_tb, con=engine, if_exists='replace')




# try:
#     conn = connection.connect(host=host, database=database, user=user, passwd=passwd)
#     # engine = pg.connect("dbname='beneficiary_db' user='dbreaduser' host='3.7.81.85' port='5432' password='DBPROD@read$#$'")
#     # df = pd.read_sql('select * from beneficiaries ', con=engine)
#     df = pd.read_sql(' select count(id) from xyz ', conn)
#     print(df)
#     print(type(df))


# except Exception as e:
#     print(str(e))
