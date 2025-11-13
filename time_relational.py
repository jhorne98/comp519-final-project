import timeit
import sys

import mariadb

from tables import QUERY_RUNS, DBLength, DBType, DBTypePostgres
import configs

def time_mariadb_queries():
    conn = None
    cursor = None

    # Connect to the mariadb service
    try:
        conn = mariadb.connect(**configs.mariadb_config)
        curs = conn.cursor()

        results = []

        try:
            for length in DBLength:
                for type in DBType:
                    table_name = length.name.lower() + "_" + type.name.lower()

                    query = "SELECT COUNT(*) FROM " + table_name + " i WHERE i.parent IS NULL AND NOT EXISTS (SELECT 1 FROM " + table_name + " t WHERE i.id = t.parent)"
                    curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                    results.append((table_name, "S0", curs_time/QUERY_RUNS))
                    print(table_name, "S0")

                    max_recursions = [4,128,256]

                    for idx, recur in enumerate(max_recursions):
                        curs.execute("SET SESSION max_recursive_iterations = " + str(recur))
                        query = "WITH RECURSIVE cte_parent_child AS (SELECT id FROM " + table_name + " WHERE parent IS NULL UNION ALL SELECT t.id FROM " + table_name + " t INNER JOIN cte_parent_child cte ON t.parent = cte.id) SELECT COUNT(*) from cte_parent_child"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "S" + str(recur), curs_time/QUERY_RUNS))
                        print(table_name, "S"+str(recur))

                    if type is DBType.INTEGER:
                        query = "SELECT COUNT(*) FROM " + table_name + " WHERE payload = 65535"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I1", curs_time/QUERY_RUNS))
                        print(table_name, "I1")

                        query = "SELECT COUNT(*) FROM " + table_name + " WHERE payload < 32767"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I2", curs_time/QUERY_RUNS))
                        print(table_name, "I2")
                    else:
                        query = "SELECT COUNT(*) FROM " + table_name + " WHERE payload LIKE '%asdf%'"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "C1", curs_time/QUERY_RUNS))
                        print(table_name, "C1")
            for line in results:
                print(line)

        except mariadb.Error as e:
            print(f"Error creating table: {e}")
            conn.rollback()
    except mariadb.Error as e:
        print(f"Error occured in MariaDB connection: {e}")
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    time_mariadb_queries()