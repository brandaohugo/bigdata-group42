from abc import abstractmethod


from mariadb import connect, Error

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class DBConnector:
    def __init__(self, hostname, username, password, port):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port

    def evaluate_query(self, sql_query, limit, n_iterations, profiling):
        for i in range(n_iterations):
            execution_query, results, elaspsed_time = self.execute_query(sql_query, limit)
            if limit not in profiling[sql_query].keys():
                profiling[sql_query][limit] = []
            profiling[sql_query][limit].append(elaspsed_time)
        return results, profiling

    @abstractmethod
    def execute_query(self, sql_query, limit):
        pass

    @abstractmethod
    def connect_to_db(self, database):
        pass


class MariaDBConnector(DBConnector):

    def connect_to_db(self,database):
        try:
            conn = connect(
                host=self.hostname,
                user=self.username,
                password=self.password,
                database=database)
            
            self.conn = conn
            self.cur = conn.cursor()
            self.cur.execute("SET PROFILING=1;")
            self.cur.execute("SET PROFILING_HISTORY_SIZE=1;")
            print("Connected to MariaDB {}:{}/{}".format(self.hostname,self.port,database))
        except Error as e:
            print("Could not connect to MariaDB {}:{}/{}".format(self.hostname,self.port,database))
            print(e)

    def close_connection(self):
        self.conn.close()

    def _get_execution_time(self):
        self.cur.execute("SHOW profiles;")
        all_results = self.cur.fetchall()
        assert len(all_results) == 1
        return all_results[0][1]

    def execute_query(self, sql_query, limit):
        results = []
        execution_query = sql_query + " LIMIT {}".format(limit)
        self.cur.execute(execution_query)
        for result in self.cur:
            results.append(result)
        elapsed_time = self._get_execution_time()
        return execution_query, results, elapsed_time


class MongoDBConnector(DBConnector):

    def connect_to_db(self, database):
        try:
            mongo_client = mongo_client = MongoClient(
                self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                authSource=database)
            self.client = mongo_client
            self.database = self.client[database]
            print("Connected to MongoDB {}:{}/{}".format(self.hostname,self.port,database))
            return self.database
        except ConnectionFailure as e:
            print("Could not connect to MongoDB {}:{}/{}".format(self.hostname,self.port,database))
            print(e)
    
    def close_connection(self):
        self.conn.close_connection()

    def execute_query(self, sql_query, limit):
        return super().execute_query(sql_query, limit)
   