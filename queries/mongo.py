import pandas as pd
from pymongo import MongoClient
import json

# DB connectivity
client = MongoClient('localhost', 27017)
db = client.db
collection = db.collection


#with open("C://Users//laura//PycharmProjects//bigdata-group42//Users.json", "r") as f:
#    users_data = json.loads(f.read())

# json_file_path = "C://Users//laura//PycharmProjects//bigdata-group42//Users.json"

# jas = open(json_file_path, encoding="utf-8").read()
# users_data = json.load(jas)


# collection.insert_many('users_data')


def csv_to_json(filename):
    data = pd.read_csv(filename)
    return data.to_dict('records')


collection.insert_many(csv_to_json("C://Users//laura//PycharmProjects//bigdata-group42//Users.csv"))


myquery = {"DisplayName": {"$regex": "^S"}}

mydoc = collection.find(myquery)

for x in mydoc:
  print(x)
