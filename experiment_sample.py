from mariadb import connect, Error
import timeit
from statistics import mean, stdev

def connect_to_db():
    try:
        conn = connect(
            host="relational.fit.cvut.cz",
            user="guest",
            password="relational",
            database="stats")
        return conn
    except Error as e:
        print(e)

def time_query(cur, sql_query, n):
    results = []
    times = []
    print("Executing: " + sql_query)
    for i in range(n):
        start_time = timeit.default_timer()
        cur.execute(sql_query)
        elapsed = timeit.default_timer() - start_time
        times.append(elapsed)
    for row in cur:
        results.append(row)
    mean_time = mean(times)
    std_time = stdev(times)
    print("Query execution returned " +  str(len(results)) + " results in " + str(round(mean_time,5)) + " seconds in average with " + str(round(std_time,5)) + " standard deviation.")
    return results, elapsed, mean_time, std_time


if __name__ == "__main__":
    conn = connect_to_db()
    cur = conn.cursor()
    sql_query = "SELECT u.DisplayName, b.Name FROM users as u, badges as b WHERE b.id = u.id LIMIT 100;"
    _,__,___,____ = time_query(cur, sql_query, 100)
    conn.close()