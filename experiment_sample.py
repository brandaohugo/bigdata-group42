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

def time_query(cur, sql_query, limit, n):
    results = []
    times = []
    print("Executing: " + sql_query.format(limit) + " {} times".format(n))
    for i in range(n):
        start_time = timeit.default_timer()
        cur.execute(sql_query.format(limit))
        elapsed = timeit.default_timer() - start_time
        times.append(elapsed)
    for row in cur:
        results.append(row)
    mean_time = mean(times)
    std_time = stdev(times)
    print("Query execution returned " +  str(len(results)) + " results in " + str(round(mean_time,5)) + " seconds in average with " + str(round(std_time,5)) + " standard deviation.")
    return len(results), mean_time, std_time


if __name__ == "__main__":
    conn = connect_to_db()
    cur = conn.cursor()
    n_iterations = 1000
    with open('output.csv','w') as f:
        f.write('n,limit,results,mean,std\n')

    for limit in (range(0,35001,1000)):
        if limit == 0: continue
        sql_query = "SELECT u.DisplayName, b.Name FROM users as u, badges as b WHERE b.id = u.id LIMIT {};"
        results, mean_time, std_time = time_query(cur, sql_query, limit, n_iterations)
        with open('output.csv','a') as f:
            f.write('{},{},{},{},{}\n'.format(n_iterations,limit,results,mean_time,std_time))
    conn.close()