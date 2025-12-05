import timeit
import sys
import random
import string
from datetime import datetime

import mariadb
import psycopg2

from tables import QUERY_RUNS, C_MAX_LENGTH, MAX_RECUR, DBLength, DBType, DBTypePostgres
from helpers import compute_avg_query_time_ms, write_data_to_csv
import configs

def time_mariadb_oqgraph_queries():
    conn = None
    cursor = None

    # Connect to the mariadb service
    try:
        conn = mariadb.connect(**configs.mariadb_oqgraph_config)
        curs = conn.cursor()

        results = []

        try:
            curs.execute("INSTALL SONAME 'ha_oqgraph'")
        except mariadb.Error as e:
            print(f"Error setting oqgraph: {e}")
            conn.rollback()

        try:
            for length in DBLength:
                for type in DBType:
                    table_name = length.name.lower() + "_" + type.name.lower()

                    query = "SELECT COUNT(*) FROM " + table_name + "_graph i WHERE i.origid = 0 AND NOT EXISTS (SELECT 1 FROM " + table_name + "_graph t WHERE i.destid = t.origid)"
                    curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                    results.append((table_name, "S0", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                    print(table_name, "S0")

                    for idx, recur in enumerate(MAX_RECUR):
                        curs.execute("SET SESSION max_recursive_iterations = " + str(recur))
                        query = "WITH RECURSIVE cte_parent_child AS (SELECT destid FROM " + table_name + "_graph WHERE origid = 0 UNION ALL SELECT t.destid FROM " + table_name + "_graph t INNER JOIN cte_parent_child cte ON t.origid = cte.destid) SELECT COUNT(*) from cte_parent_child"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "S" + str(recur), compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "S"+str(recur))

                    if type is DBType.INTEGER:
                        query = "SELECT COUNT(*) FROM " + table_name + "_backing WHERE payload = " + str(random.randint(0, 65536))
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I1", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I1")

                        query = "SELECT COUNT(*) FROM " + table_name + "_backing WHERE payload < " + str(random.randint(0, 65536))
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I2", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I2")

                        query = "SELECT COUNT(*) FROM " + table_name + "_backing WHERE MOD(payload, " + str(random.randint(0, 65536)) + ") = 0"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I3", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I3")

                        '''
                        ids = []
                        while not ids:
                            query_ids = "SELECT destid FROM " + table_name + "_backing WHERE payload = " + str(random.randint(0, 65536))
                            curs.execute(query_ids)
                            for row in curs:
                                ids.append(str(row[0]))
                        ids = ','.join(ids)
                        '''
                        query = "SELECT AVG(seq) FROM " + table_name + "_graph WHERE latch='breadth_first' AND origid=0 AND destid IN (" + str(random.randint(0, length.value)) + ") AND destid = linkid"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "R1", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "R1")

                    else:
                        for c in range(1,C_MAX_LENGTH+1):
                            query = "SELECT COUNT(*) FROM " + table_name + "_backing WHERE payload LIKE '%" + ''.join(random.choices(string.ascii_letters + string.digits, k=c)) + "%'"
                            curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                            results.append((table_name, "C"+str(c), c, compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                            print(table_name, "C"+str(c))

                        '''
                        query_ids = "SELECT destid FROM " + table_name + "_backing WHERE payload LIKE '%" + ''.join(random.choices(string.ascii_letters + string.digits, k=4)) + "%'"
                        curs.execute(query_ids)
                        ids = []
                        for row in curs:
                            ids.append(str(row[0]))
                        ids = ','.join(ids)
                        '''
                        query = "SELECT AVG(seq) FROM " + table_name + "_graph WHERE latch='breadth_first' AND origid=0 AND destid IN (" + str(random.randint(0, length.value)) + ") AND destid = linkid"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "R1", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "R1")
            for line in results:
                print(line)
            write_data_to_csv(results, "test/oqgraph_random_" + datetime.today().strftime('%Y_%m_%d') + ".csv")

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

def time_apache_age_queries():
    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**configs.apache_age_config)
        curs = conn.cursor()

        #curs.execute("CREATE EXTENSION age")
        curs.execute("LOAD 'age'")
        curs.execute("SET search_path=ag_catalog, '$user', public;")
        conn.commit()

        results = []

        try:
            for length in DBLength:
                for type in DBTypePostgres:
                    table_name = length.name.lower() + "_" + type.name.lower()

                    #query = "SELECT COUNT(*) FROM " + table_name + " i WHERE i.parent IS NULL AND NOT EXISTS (SELECT 1 FROM " + table_name + " t WHERE i.id = t.parent)"
                    query = "WITH nodes_with_vertices AS (" + \
                    "SELECT * FROM cypher('" + table_name + "', $$MATCH (n:Node)-[:PARENT_OF]->() RETURN n$$) AS (v agtype) " + \
                    "UNION " + \
                    "SELECT * FROM cypher('" + table_name + "', $$MATCH ()-[:PARENT_OF]->(n:Node) RETURN n$$) AS (n2 agtype)" + \
                    ")," + \
                    "all_nodes AS (" + \
                    "SELECT * FROM cypher('" + table_name + "', $$MATCH (n:Node) RETURN n$$) AS (f agtype)" + \
                    ")" + \
                    "SELECT COUNT(*) FROM all_nodes LEFT JOIN nodes_with_vertices ON all_nodes.f = nodes_with_vertices.v WHERE nodes_with_vertices.v IS NULL;"
                    curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                    results.append((table_name, "S0", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                    print(table_name, "S0")

                    for idx, recur in enumerate(MAX_RECUR):
                        #query = "WITH RECURSIVE cte_parent_child AS (SELECT id FROM " + table_name + " WHERE parent IS NULL UNION ALL SELECT t.id FROM " + table_name + " t INNER JOIN cte_parent_child cte ON t.parent = cte.id) SELECT COUNT(*) from cte_parent_child"
                        query = "WITH nodes_with_in_vertices AS (" + \
                        "SELECT * FROM cypher('" + table_name + "', $$MATCH ()-[:PARENT_OF]->(n:Node) RETURN n$$) AS (v agtype)" + \
                        ")," + \
                        "all_nodes AS (" + \
                        "SELECT * FROM cypher('" + table_name + "', $$MATCH (n:Node)-[x*0.." + str(recur) + "]->(o:Node) RETURN n$$) AS (f agtype)" + \
                        ")" + \
                        "SELECT COUNT(*) FROM all_nodes LEFT JOIN nodes_with_in_vertices ON all_nodes.f = nodes_with_in_vertices.v WHERE nodes_with_in_vertices.v IS NULL;"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "S" + str(recur), compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "S"+str(recur))

                    if type is DBTypePostgres.INTEGER:
                        query = "SELECT * FROM cypher('" + table_name + "', $$MATCH (n:Node) WITH n, COUNT(*) AS _ WHERE n.payload = " + str(random.randint(0, 65536)) + " RETURN n$$) AS (n agtype);"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I1", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I1")

                        query = "SELECT * FROM cypher('" + table_name + "', $$MATCH (n:Node) WITH n, COUNT(*) AS _ WHERE n.payload > " + str(random.randint(0, 65536)) + " RETURN n$$) AS (n agtype);"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I2", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I2")

                        query = "SELECT * FROM cypher('" + table_name + "', $$MATCH (n:Node) WITH n, COUNT(*) AS _ WHERE toInteger(n.payload) % " + str(random.randint(0, 65536)) + " = 0 RETURN n$$) AS (n agtype);"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "I3", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "I3")

                        query = "SELECT AVG(CAST(d as int)) FROM cypher('" + table_name + "', $$MATCH (n:Node)<-[x:PARENT_OF*0..]-(o:Node) WHERE n.payload = '" + str(random.randint(0, 65536)) + "' RETURN n, COUNT(o) AS depth$$) AS (n agtype, d agtype);"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "R1", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "R1")
                    else:
                        for c in range(1,C_MAX_LENGTH+1):
                            query = "SELECT * FROM cypher('" + table_name + "', $$MATCH (n:Node) WITH n, COUNT(*) AS _ WHERE n.payload CONTAINS '" + ''.join(random.choices(string.ascii_letters + string.digits, k=c)) + "' RETURN n$$) AS (n agtype);"
                            curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                            results.append((table_name, "C"+str(c), compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                            print(table_name, "C"+str(c))

                        query = "SELECT AVG(CAST(d as int)) FROM cypher('" + table_name + "', $$MATCH (n:Node)<-[x:PARENT_OF*0..]-(o:Node) WHERE n.payload = '" + ''.join(random.choices(string.ascii_letters + string.digits, k=4)) + "' RETURN n, COUNT(o) AS depth$$) AS (n agtype, d agtype);"
                        curs_time = timeit.timeit(lambda: curs.execute(query), number=QUERY_RUNS)
                        results.append((table_name, "R1", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "R1")
            for line in results:
                print(line)
            write_data_to_csv(results, "test/apache_age_random" + datetime.today().strftime('%Y_%m_%d') + ".csv")

        except (Exception, psycopg2.Error) as e:
            print(f"Error executing graph: {e}")
            conn.rollback()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error occured in PostgreSQL connection: {e}")
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()