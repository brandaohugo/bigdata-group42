import pandas as pd
from pymongo import MongoClient
import timeit
from statistics import mean, stdev

client = MongoClient('localhost', 27017)
db = client.db
Users = db.Users
Comments = db.Comments

results= []
times = []

for i in range(1000):
    start_time = timeit.default_timer()
    myquery = {"DisplayName": {"$regex": "^S"}}
    mydoc = Users.find(myquery)
    elapsed = timeit.default_timer() - start_time
    print(elapsed)
    times.append(elapsed)
mean_time = mean(times)
std_time = stdev(times)
print(mean_time, std_time)
