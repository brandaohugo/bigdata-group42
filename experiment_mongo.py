from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import timeit


DB_USERNAME = "pi"
DB_PASSWORD = "raspberry"
MONGODB_HOSTNAME = "mongodb://ubuntu.local"
MONGODB_PORT = 27017

class DBConnector:
    def __init__(self, hostname, username, password, port):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port

class MongoDBConnector(DBConnector):

    def connect_to_db(self):
        try:
            mongo_client = mongo_client = MongoClient(
                self.hostname,
                username=self.username,
                password=self.password,
                port=self.por, datat,
                authSource="admin")
            return mongo_client
        except ConnectionFailure as e:
            print(e)
        


if __name__ == "__main__":
    mongdb_conn = MongoDBConnector(
        MONGODB_HOSTNAME,
        DB_USERNAME,
        DB_PASSWORD,
        MONGODB_PORT
    ).connect_to_db()
    from pprint import pprint
    stats_db = mongdb_conn['admin']
    user_collection = stats_db['system.users']
    print(user_collection.find().explain()['executionStats']['executionTimeMillis'])
    # start_time = timeit.default_timer()
    # print(user_collection.find_one({"user":"pi"}))
    # print(timeit.default_timer() - start_time)