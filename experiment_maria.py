from mariadb import connect, Error
import timeit
from statistics import mean, stdev
from abc import abstractmethod
from pprint import pprint
from os import listdir, getcwd
from os.path import join as path_join

DB_USERNAME = "pi"
DB_PASSWORD = "raspberry"
MARIADB_HOSTNAME = "raspberrypi.local"
MARIADB_PORT = 3306

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
        except Error as e:
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


def weak_scaling_profiling(db_connector, sql_query, max_limit, increment_limit, n_iterations, profiling):
    for limit in (range(0,max_limit+1, increment_limit)):
        if limit == 0: continue
        results, profiling = db_connector.evaluate_query(sql_query, limit, n_iterations, profiling)
        if len(results) < limit:
            del profiling[sql_query][limit]
            break
    return profiling


def read_queries(queries_directory):
    queries_dir = path_join(getcwd(),queries_directory)
    queries = []
    for filename in listdir(queries_dir):
        query_path = path_join(queries_dir,filename)
        query_string = open(query_path).read().replace("\n", " ")
        queries.append(query_string)
    return queries

if __name__ == "__main__":
    db_connector = MariaDBConnector(MARIADB_HOSTNAME, DB_USERNAME, DB_PASSWORD, MARIADB_PORT)
    db_connector.connect_to_db("stats")

    n_iterations = 1
    max_limit = 10000
    increment_limit = 10000

    profiling = {}
    
    for sql_query in read_queries("queries"):
        if sql_query not in profiling.keys():
            profiling[sql_query] = {}
        profiling = weak_scaling_profiling(db_connector, sql_query, max_limit, increment_limit, n_iterations, profiling)
    pprint(profiling)

    db_connector.close_connection()