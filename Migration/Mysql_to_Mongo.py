import json
from urllib.parse import quote_plus
from decimal import Decimal
from bson.decimal128 import Decimal128
#import MySQL Library in Python
from bson import BSON
import mysql.connector as connection
import mysql.connector as conn
import pandas as pd
import pymongo
from pandas.io import sql
from pymongo import MongoClient
from pymongo.server_api import ServerApi
# import dnspython

# conn = conn.connect(host="localhost", database="nb3", user="root", passwd="", )

# df = pd.read_sql(' select * from beneficiary_address group by state', conn)

# cursor = conn.cursor(dictionary=True)
#
# cursor.execute("SELECT * from xyz")
# myresult = cursor.fetchall()



mysql_conn = connection.connect(host='localhost', database='mydatabase', user='root', passwd='')
cursor = mysql_conn.cursor()
query = "SELECT DATA_TYPE,COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_schema = 'mydatabase' and table_name = N'payments' order by column_name"
cursor.execute(query)
recor=cursor.fetchall()
print(recor)
for i in range(len(recor)):
    #converts 'date' datatype to 'datatime'

    if recor[i][0]=='date' or 'timestamp' or 'time' or 'year':
        print(recor[i])
        query="ALTER TABLE payments MODIFY "+recor[i][1]+" datetime"
        cursor.execute(query)

# for j in range(len(recor)):
#     if recor[j][0]=='decimal' or 'double' or 'real':
#         print(recor[j][0])
#         query="ALTER TABLE payments MODIFY "+recor[j][1]+" float"
#         cursor.execute(query)

cursor = mysql_conn.cursor()
query = "SELECT * FROM payments limit 10"
cursor.execute(query)

# Fetch the rows from the cursor and store them in a Pandas dataframe
df = pd.DataFrame(cursor.fetchall())
df.columns = [col[0] for col in cursor.description]
print(df)
# rows = cursor.fetchall()
# print(rows)
# df1 = my.reset_index(drop=True)
# new=json.dumps(my)
# print(my)
# my.dropna(inplace = True)

# df=my.to_dict()
# df1 = df.reset_index(drop=True)
# print(df)
# mr=pd.r
# print(mr)
# mongodb_host = "mongodb+srv://Darshpatil:<password>@cluster0.7uiopps.mongodb.net/test
# a=json.dumps(myresult)
# mongodb_dbname = "first2"

username = quote_plus('Darshpatil')
password = quote_plus('Mondarsh1@')
client = pymongo.MongoClient("mongodb+srv://"+username+":"+password+"@cluster0.7uiopps.mongodb.net/test?retryWrites=true&w=majority",
                             server_api=ServerApi('1'))
# mongodb+srv://Darshpatil:<password>@cluster0.7uiopps.mongodb.net/test
print("dsc")
# print(client.list_database_names())

# db = client.test

db = client["new"]
collection = db["last"]


# myclient = pymongo.MongoClient(client)
#
# mydb = myclient[mongodb_dbname]
#
# mycol = mydb["f"]
#
# if len(df) > 0:
#     x = collection.insert_many(df)  # myresult comes from mysql cursor
# mongoImp = dbo.insert_many(odbcArray)


def convert_decimal(dict_item):
    # This function iterates a dictionary looking for types of Decimal and converts them to Decimal128
    # Embedded dictionaries and lists are called recursively.
    if dict_item is None: return None

    for k, v in list(dict_item.items()):
        if isinstance(v, dict):
            convert_decimal(v)
        elif isinstance(v, list):
            for l in v:
                convert_decimal(l)
        elif isinstance(v, Decimal):
            dict_item[k] = Decimal128(str(v))

    return dict_item

# js=df.to_string(index=False)
# print(type(js))

# js=df.to_string()
# document = js
# bson_data = BSON.encode(document)
# json_dict = json.loads(js,use_decimal=True)
# ds=df.to_dict()
# document = df.to_()
# bson_data = BSON.encode(document)
# df.apply(pd.to_datetime(df))

# records = json.loads(df.T.to_json()).values()
# collection.insert_many(records)

for index, row in df.iterrows():
    db.last.insert_one(row.to_dict())


print("jhg")
    # print(len(x.inserted_ids))
