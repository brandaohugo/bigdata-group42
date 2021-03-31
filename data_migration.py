from db_utils import MariaDBConnector, MongoDBConnector
import os
import datetime

DB_USERNAME = "pi"
DB_PASSWORD = "raspberry"
DB_NAME = "stats"

MARIADB_HOSTNAME = "raspberrypi.local"
MARIADB_PORT = 3306

MONGODB_HOSTNAME = "mongodb://ubuntu.local"
MONGODB_PORT = 27017

STATS_DENORM_SCHEMA = {
    "posts": { 
        "embed": ["postHistory", "postLinks", "comments", "votes"],
        "id_label": "PostId"
    },
    "users": {
        "embed": ["comments", "badges", "postHistory", "votes"],
        "id_label": "UserId"
    }
}


def convert_to_dict(results, cols):
    result_dicts = []
    for i in range(len(results)):
        row_dict = dict(zip(cols, results[i]))
        result_dicts.append(row_dict)
        
    date_col = None
    if len(results):
        for k in range(len(results[0])):
            if type(results[0][k]) == datetime.date:
                date_col = cols[k]
        
    if date_col:
        result_dicts = [dict(r,**{date_col:datetime.datetime.combine(r[date_col],datetime.datetime.min.time())}) for r in result_dicts]

    return result_dicts

def get_maria_table_names(conn):
    sql_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='stats'"
    results = conn.execute_query(sql_query, 999999)[1] 
    return [r[0] for r in results]

def get_maria_table_col_names(conn, database, table):
    sql_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{}' AND TABLE_SCHEMA = '{}'".format(table,database)
    results = conn.execute_query(sql_query, 999999)[1]
    return ["_id" if r[0] == "Id" else r[0] for r in results]

def run_migration(mariadb_conn, mongodb_conn, db_name):
    tables = get_maria_table_names(mariadb_conn)
    for table in tables:
        cols = get_maria_table_col_names(mariadb_conn,db_name, table)
        results = mariadb_conn.execute_query("SELECT * FROM {}".format(table),9999999999999999)[1]
        insert_dict = convert_to_dict(results,cols)
        collection = table
        mongodb_conn[collection].insert_many(insert_dict)
        assert mongodb_conn[collection].find().count() == len(results)
        print("Migrated {} rows from {}".format(len(results),table))


def select_objects_in_dict(list_of_dicts,key,value):
    results = []
    for d in list_of_dicts:
        if d[key] == value:
            results.append(d)
    return results

def denormalize_database(db_conn, db_name, db_schema):

    for collection in list(db_schema.keys()):
        doc_count = 1
        collection_docs = list(db_conn[collection].find())
        collection_length = len(collection_docs)
        
        embed_data = {}
        fields = db_schema[collection]["embed"]
        for embed in fields:
            embed_data[embed] = list(db_conn[embed].find())

        for document in collection_docs:
            doc_id = document["_id"]
            print("Processing {} document {} ({} of {})".format(collection, doc_id, doc_count, collection_length))
            query = {"_id": doc_id}
            id_label = db_schema[collection]["id_label"]
            values = [select_objects_in_dict(embed_data[field],id_label,doc_id) for field in fields]
            new_values = {"$set" : dict(zip(fields,values))}
            db_conn[collection].update_one(query, new_values)
            doc_count +=1
            

if __name__ == "__main__":
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
    
    db_conn = mongodb_conn.connect_to_db(DB_NAME)

    # mongodb_conn.client.drop_database(DB_NAME)
    # run_migration(mariadb_conn, mongodb_stats, DB_NAME)
    
    # denormalize_database(db_conn, DB_NAME, STATS_DENORM_SCHEMA)  

    
