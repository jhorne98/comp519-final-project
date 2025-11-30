import timeit
import sys
import random
import string

import mariadb
import psycopg2

from tables import QUERY_RUNS, C_MAX_LENGTH, MAX_RECUR, DBLength, DBType, DBTypePostgres
from helpers import compute_avg_query_time_ms
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
                    results.append((table_name, "S0", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                    print(table_name, "S0")

                    for idx, recur in enumerate(MAX_RECUR):
                        curs.execute("SET SESSION max_recursive_iterations = " + str(recur))
                        query = "WITH RECURSIVE cte_parent_child AS (SELECT id FROM " + table_name + " WHERE parent IS NULL UNION ALL SELECT t.id FROM " + table_name + " t INNER JOIN cte_parent_child cte ON t.parent = cte.id) SELECT COUNT(*) from cte_parent_child"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "S" + str(recur), compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "S"+str(recur))

                    if type is DBType.INTEGER:
                        query = "SELECT COUNT(*) FROM " + table_name + " WHERE payload = " + str(random.randint(0, 65536))
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I1", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I1")

                        query = "SELECT COUNT(*) FROM " + table_name + " WHERE payload < " + str(random.randint(0, 65536))
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I2", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I2")

                        query = "SELECT COUNT(*) FROM " + table_name + " WHERE MOD(payload, " + str(random.randint(0, 65536)) + ") = 0"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I3", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I3")
                    else:
                        for c in range(1,C_MAX_LENGTH+1):
                            query = "SELECT COUNT(*) FROM " + table_name + " WHERE payload LIKE '%" + ''.join(random.choices(string.ascii_letters + string.digits, k=c)) + "%'"
                            curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                            results.append((table_name, "C1", c, compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                            print(table_name, "C1", c)
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

def time_postgres_queries():
    conn = None
    cursor = None

    # Connect to the postgres service
    try:
        conn = psycopg2.connect(**configs.postgres_config)
        curs = conn.cursor()

        results = []

        try:
            for length in DBLength:
                for type in DBTypePostgres:
                    table_name = length.name.lower() + "_" + type.name.lower()

                    query = "SELECT COUNT(*) FROM " + table_name + " i WHERE i.parent IS NULL AND NOT EXISTS (SELECT 1 FROM " + table_name + " t WHERE i.id = t.parent)"
                    curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                    results.append((table_name, "S0", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                    print(table_name, "S0")

                    for idx, recur in enumerate(MAX_RECUR):
                        str_recur = str(recur)
                        query = "WITH RECURSIVE cte_parent_child AS (SELECT id, 0 AS depth FROM " + table_name + " WHERE parent IS NULL UNION ALL SELECT t.id, depth + 1 FROM " + table_name + " t INNER JOIN cte_parent_child cte ON t.parent = cte.id WHERE depth < " + str_recur + ") SELECT COUNT(*) from cte_parent_child"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "S" + str_recur, compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "S"+str_recur)

                    if type is DBTypePostgres.INTEGER:
                        query = "SELECT COUNT(*) FROM " + table_name + " WHERE payload = " + str(random.randint(0, 65536))
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I1", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I1")

                        query = "SELECT COUNT(*) FROM " + table_name + " WHERE payload < " + str(random.randint(0, 65536))
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I2", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I2")

                        query = "SELECT COUNT(*) FROM " + table_name + " WHERE MOD(payload, " + str(random.randint(0, 65536)) + ") = 0"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I2", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I2")
                    else:
                        for c in range(1,C_MAX_LENGTH+1):
                            query = "SELECT COUNT(*) FROM " + table_name + " WHERE payload LIKE '%" + ''.join(random.choices(string.ascii_letters + string.digits, k=c)) + "%'"
                            curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                            results.append((table_name, "C1", c, compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                            print(table_name, "C1", c)
            for line in results:
                print(line) 
        except (Exception, psycopg2.Error) as e:
            print(f"Error creating table: {e}")
            conn.rollback()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error occured in MariaDB connection: {e}")
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close() 