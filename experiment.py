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

N_ITERATIONS = 1000
MAX_LIMIT = 10000
INCREMENT_LIMIT = 100

SELECT_FILTER_JOIN_QUERIES = [
    "select_comments",
    # "filter_users_by_upvote",
    # "select_tag_max_count",
    # "sort_posts_by_viewcount",
    # "filter_comments_by_id",
    # "average_post_fav_count",
    # "sum_users_downvotes",
    # "count_users_by_age",
    # "update_users_name",
]

INSERT_QUERIES = [
    "insert_badges"
]

DELETE_QUERIES = [
    "drop_badges",
    "delete_user_badges",
]

# queries not implemented in the experiment
# query_name =  # Mongo Error
# query_name =  # Mongo error
# query_name = # Not implemented in Mongo
# query_name = "count_votes_bounty" #  Not implemented in Mongo
# query_name = "select_owner_not_null" #  Not implemented in Mongo


def get_table_last_id(mariadb_conn,table):
    _,results, _ = mariadb_conn.execute_query("SELECT Id FROM {} ORDER BY ID DESC LIMIT 1;".format(table))
    return results[0][0]

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

def run_filter_join_queries(mariadb_conn,mongodb_conn,results_fname):
    
    for query_name in SELECT_FILTER_JOIN_QUERIES:
        for i in range(N_ITERATIONS):
            print("Executing {} iteration {} of {}".format(query_name, i+1, N_ITERATIONS))
            _, maria_results, maria_exec_milli = mariadb_conn.execute_query(get_query_by_name(queries_list, query_name)["maria"])
            _, mongo_results, mongo_exec_milli = mongodb_conn.execute_query(get_query_by_name(queries_list, query_name)["mongo"])
            maria_n_results = len(maria_results)
            mongo_n_results = len(mongo_results)
            append_to_csv(results_fname,[query_name, i+1 ,maria_exec_milli, mongo_exec_milli, len(maria_results)])
        


def run_insert_queries(mariadb_conn,mongodb_conn,results_fname):
    print("Running insert queries")
    print("Initial MongoDB: " ,mongodb_conn.database['badges'].find().count())
    mariadb_conn.cur.execute("SELECT count(*) FROM badges;")
    print("Initial Maria:", mariadb_conn.cur.fetchall()[0][0] )
   
    results_fname = "./results/query_times.csv"
    for query_name in INSERT_QUERIES:
        last_id = get_table_last_id(mariadb_conn, "badges")
        for i in range(N_ITERATIONS):
            print("Executing {} iteration {} of {}".format(query_name, i+1, N_ITERATIONS))
            last_id += 1
            maria_query = get_query_by_name(queries_list, query_name)["maria"].format(last_id)
            # print(maria_query)
            mongo_query = get_query_by_name(queries_list, query_name)["mongo"]
            _, maria_results, maria_exec_milli = mariadb_conn.execute_query(maria_query)
            _, mongo_results, mongo_exec_milli = mongodb_conn.execute_query(mongo_query)
            append_to_csv(results_fname,[query_name, i+1 ,maria_exec_milli, mongo_exec_milli, len(maria_results)])
    print("Final MongoDB: " ,mongodb_conn.database['badges'].find().count())
    mariadb_conn.cur.execute("SELECT count(*) FROM badges;")
    print("Final Maria:", mariadb_conn.cur.fetchall()[0][0] )

def run_delete_queries(mariadb_conn,mongodb_conn,results_fname):
    print("Running delete queries")
    print("Initial MongoDB: " ,mongodb_conn.database['badges'].find().count())
    mariadb_conn.cur.execute("SELECT count(*) FROM badges;")
    print("Initial Maria:", mariadb_conn.cur.fetchall()[0][0] )
   
    results_fname = "./results/query_times.csv"
    for query_name in DELETE_QUERIES:
        for i in range(N_ITERATIONS):
            print("Executing {} iteration {} of {}".format(query_name, i+1, N_ITERATIONS))
            maria_query = get_query_by_name(queries_list, query_name)["maria"]
            mongo_query = get_query_by_name(queries_list, query_name)["mongo"]
            _, maria_results, maria_exec_milli = mariadb_conn.execute_query(maria_query)
            _, mongo_results, mongo_exec_milli = mongodb_conn.execute_query(mongo_query)
            append_to_csv(results_fname,[query_name, i+1 ,maria_exec_milli, mongo_exec_milli, len(maria_results)])
            print("Final MongoDB: " ,mongodb_conn.database['badges'].find().count())
            mariadb_conn.cur.execute("SELECT count(*) FROM badges;")
            print("Final Maria:", mariadb_conn.cur.fetchall()[0][0] )
            db_initialization(maria=False, mongo=False, n_iterations=1)
    


if __name__ == "__main__":

    db_initialization(maria=True, mongo=True, n_iterations=1)

    mariadb_conn = MariaDBConnector(
        MARIADB_HOSTNAME, 
        DB_USERNAME,
        DB_PASSWORD, 
        MARIADB_PORT
    )
    mariadb_conn.connect_to_db(DB_NAME)
    
    mongodb_conn = MongoDBConnector(
        MONGODB_HOSTNAME, 
        DB_USERNAME,
        DB_PASSWORD, 
        MONGODB_PORT
    )
    mongodb_conn.connect_to_db(DB_NAME)
     
    results_fname = "./results/query_times.csv"
    append_to_csv(results_fname,["query_name", "iteration", "maria_exec_milli", "mongo_exec_milli", "n_results"])
    
    run_filter_join_queries(mariadb_conn,mongodb_conn,results_fname)
    run_insert_queries(mariadb_conn,mongodb_conn,results_fname)
    run_delete_queries(mariadb_conn,mongodb_conn,results_fname)
    