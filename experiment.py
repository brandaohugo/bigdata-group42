from pprint import pprint
from os import listdir, getcwd
from os.path import join as path_join
from db_utils import MariaDBConnector, MongoDBConnector
import json
import timeit
import paramiko
import csv


DB_USERNAME = "pi"
DB_PASSWORD = "raspberry"
DB_NAME = "stats"

MARIADB_HOSTNAME = "raspberrypi.local"
MARIADB_PORT = 3306

MONGODB_HOSTNAME = "mongodb://ubuntu.local"
MONGODB_PORT = 27017

N_ITERATIONS = 100
MAX_LIMIT = 10000
INCREMENT_LIMIT = 100


def weak_scaling_profiling(db_connector, sql_query, max_limit, increment_limit, n_iterations, profiling):
    print("Running Weak Scaling on query {}".format(sql_query))
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

def save_profiling(profilig, filename):
    results_dir = path_join(getcwd(),"results")
    with open(path_join(results_dir,filename), "w") as outfile:
        json.dump(profilig, outfile, indent=4, sort_keys=True)

def run(conn, profiling_filename):

    profiling = {}
    for sql_query in read_queries("queries"):
        if sql_query not in profiling.keys():
            profiling[sql_query] = {}
        profiling = weak_scaling_profiling(
            mariadb_conn, 
            sql_query, 
            MAX_LIMIT, 
            INCREMENT_LIMIT,
            N_ITERATIONS, 
            profiling)
    
    save_profiling(profiling, profiling_filename)

    mariadb_conn.close_connection()

def time_initilization(host, user, password, command, n_iterations, out_file):
    times = []
    for i in range(n_iterations):
        elapsed = _execute_ssh_command(host, user, password, command)
        times.append([elapsed])
    with open(out_file, "w") as f:
        wr = csv.writer(f)
        wr.writerows(times)
    print("Times on {}".format(out_file))

def _execute_ssh_command(host, user, password, command):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host,username=user, password=password)
    start_time = timeit.default_timer()
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command)
    ssh_stdout.readlines()
    elapsed = timeit.default_timer() - start_time
    return elapsed

if __name__ == "__main__":

    cmd = 'mysql -upi -praspberry stats < stats.sql'
    out_file = "init_mariadb.csv"
    time_initilization(MARIADB_HOSTNAME, DB_USERNAME, DB_PASSWORD, cmd, 10, out_file)

    

    mariadb_conn = MariaDBConnector(
        MARIADB_HOSTNAME, 
        DB_USERNAME,
        DB_PASSWORD, 
        MARIADB_PORT
    )
    mariadb_conn.connect_to_db(DB_NAME)
    
    # time initializations
    


    # run(mariadb_conn, "mariadb_results.json")
    
    # mongodb_conn = MongoDBConnector(
    #     MONGODB_HOSTNAME, 
    #     DB_USERNAME,
    #     DB_PASSWORD, 
    #     MONGODB_PORT
    # )    
    # stats_db = mongodb_conn.connect_to_db("stats")
    
    
    # user_collection = stats_db['system.users']
    # print(user_collection.find().explain()['executionStats']['executionTimeMillis'])