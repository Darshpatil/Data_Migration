import json
from urllib.parse import quote_plus

#import MySQL Library in Python
import mysql.connector as conn
import pandas as pd
import pymongo
from pymongo import MongoClient
from pymongo.server_api import ServerApi
# import dnspython

conn = conn.connect(host="localhost", database="nb3", user="root", passwd="", )

# df = pd.read_sql(' select * from beneficiary_address group by state', conn)

cursor = conn.cursor(dictionary=True)

cursor.execute("SELECT * from xyz")
myresult = cursor.fetchall()

# mongodb_host = "mongodb+srv://Darshpatil:<password>@cluster0.7uiopps.mongodb.net/test
# a=json.dumps(myresult)
mongodb_dbname = "first"

username = quote_plus('Darshpatil')
password = quote_plus('Mondarsh1@')
client = pymongo.MongoClient("mongodb+srv://"+username+":"+password+"@cluster0.7uiopps.mongodb.net/?retryWrites=true&w=majority",
                             server_api=ServerApi('1'))
# print(client.list_database_names())

# db = client.test

db = client["new"]
collection = db["n"]


# myclient = pymongo.MongoClient(client)
#
# mydb = myclient[mongodb_dbname]
#
# mycol = mydb["f"]
#
# if len(myresult) > 0:
#     x = coll.insert_many(myresult)  # myresult comes from mysql cursor
# mongoImp = dbo.insert_many(odbcArray)
collection.insert_many(myresult)
    # print(len(x.inserted_ids))
