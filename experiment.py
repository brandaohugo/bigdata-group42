from pprint import pprint
from os import listdir, getcwd
from os.path import join as path_join
from db_utils import MariaDBConnector, MongoDBConnector
from utils import execute_ssh_command, append_to_csv
from queries import queries_list, get_query_by_name
import json
import ast


DB_USERNAME = "pi"
DB_PASSWORD = "raspberry"
DB_NAME = "stats"

MARIADB_HOSTNAME = "raspberrypi.local"
MARIADB_PORT = 3306

MONGODB_HOSTNAME = "mongodb://ubuntu"
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

def _time_initilization(host, user, password, command, n_iterations, out_file):
    for i in range(n_iterations):
        print("Iteration {} of {}".format(i+1,n_iterations))
        _, elapsed = execute_ssh_command(host, user, password, command)
        append_to_csv(out_file,[elapsed])
    print("Times on {}".format(out_file))

def db_initialization(maria=True, mongo=True, n_iterations=1):
    
    if maria:
        print("Initialzing MariaDB")
        cmd = 'mysql -upi -praspberry stats < stats.sql'
        out_file = "./results/init_mariadb.csv"
        _time_initilization(MARIADB_HOSTNAME, DB_USERNAME, DB_PASSWORD, cmd, n_iterations, out_file)
        print("MariaDB initialized")

    if mongo:
        print("Initializing MongoDB")
        print("Dropping database")
        execute_ssh_command("ubuntu", "ubuntu", "raspberry","mongo stats --eval 'db.dropDatabase()" )
        print("Database dropped")
        
        cmd = 'mongorestore --drop'
        out_file = "./results/init_mongo.csv"
        _time_initilization("ubuntu", "ubuntu", DB_PASSWORD, cmd, n_iterations, out_file)
        print("MongoDB initialized")

if __name__ == "__main__":

    db_initialization(maria=False, mongo=False, n_iterations=1)

    mariadb_conn = MariaDBConnector(
        MARIADB_HOSTNAME, 
        DB_USERNAME,
        DB_PASSWORD, 
        MARIADB_PORT
    )
    mariadb_conn.connect_to_db(DB_NAME)
    
    # run(mariadb_conn, "mariadb_results.json")
    
    mongodb_conn = MongoDBConnector(
        MONGODB_HOSTNAME, 
        DB_USERNAME,
        DB_PASSWORD, 
        MONGODB_PORT
    )

    # query_name = "test" # OK   
    # query_name = "filter_comments_by_id" 
    # query_name = "filter_users_by_upvote" # OK
    # query_name = "sort_posts_by_viewcount" # OK
    # query_name = "outer_join_tags_count" # Mongo error
    # query_name = "outer_join_comments_users" # OK
    # query_name = "update_users_name" # Debug results
    # query_name = "user_badges" # OK
    # query_name = "average_post_fav_count" # Check result
    # query_name = "count_votes_bounty" # Mongo not implemented
    # query_name = "select_owner_not_null" # Mongo not implemented
    # query_name = "select_tag_max_count" # Check results
    # query_name = "sum_users_downvotes" # Not tested
    # query_name = "count_users_by_age" # Not tested
    # query_name = "insert_badges" # Not tested
    # query_name = "delete_user_badges" # Not tested
    # query_name = "drop_badges" # Not tested
    
    mongodb_conn.connect_to_db(DB_NAME)
    _, maria_n_results, maria_exec_milli = mariadb_conn.execute_query(get_query_by_name(queries_list, query_name)["maria"])
    _, mongo_n_results, mongo_exec_milli = mongodb_conn.execute_query(get_query_by_name(queries_list, query_name)["mongo"])
    if maria_n_results != mongo_n_results:
        print("Queries are different!")
        print("MariaDB returned {} entries".format(maria_n_results))
        print("MongoDB returned {} entries".format(mongo_n_results))
    print("MariaDB exec: {}".format(maria_exec_milli))
    print("MongoDB exec: {}".format(mongo_exec_milli))
    # user_collection = stats_db['system.users'
    # print(user_collection.find().explain()['executionStats']['executionTimeMillis'])