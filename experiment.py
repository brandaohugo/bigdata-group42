from pprint import pprint
from os import listdir, getcwd
from os.path import join as path_join
from db_utils import MariaDBConnector, MongoDBConnector

DB_USERNAME = "pi"
DB_PASSWORD = "raspberry"

MARIADB_HOSTNAME = "raspberrypi.local"
MARIADB_PORT = 3306

MONGODB_HOSTNAME = "mongodb://ubuntu.local"
MONGODB_PORT = 27017


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


def run_maria():
    mariadb_conn = MariaDBConnector(
        MARIADB_HOSTNAME, 
        DB_USERNAME,
        DB_PASSWORD, 
        MARIADB_PORT
    )
    mariadb_conn.connect_to_db("stats")

    profiling = {}
    
    n_iterations = 1
    max_limit = 10000
    increment_limit = 10000
    
    for sql_query in read_queries("queries"):
        if sql_query not in profiling.keys():
            profiling[sql_query] = {}
        profiling = weak_scaling_profiling(mariadb_conn, sql_query, max_limit, increment_limit, n_iterations, profiling)
    pprint(profiling)

    mariadb_conn.close_connection()


if __name__ == "__main__":
    mongodb_conn = MongoDBConnector(
        MONGODB_HOSTNAME,
        DB_USERNAME,
        DB_PASSWORD,
        MONGODB_PORT
    )
    stats_db = mongodb_conn.connect_to_db("admin")
    
    
    user_collection = stats_db['system.users']
    print(user_collection.find().explain()['executionStats']['executionTimeMillis'])