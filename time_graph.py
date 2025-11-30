import timeit
import random
import string

from neo4j import GraphDatabase

from tables import QUERY_RUNS, C_MAX_LENGTH, MAX_RECUR, DBLength, DBType
from helpers import compute_avg_query_time_ms
import configs

def time_graph_queries(config):
    with GraphDatabase.driver(config['uri'], auth=config['auth']) as client:
        client.verify_connectivity()

        results = []

        for length in DBLength:
            for type in DBType:
                table_name = length.name.lower() + "_" + type.name.lower()

                query = "MATCH (n:Node) WHERE n.table = '" + table_name + "' AND NOT (n)-[:PARENT_OF]->() AND NOT ()-[:PARENT_OF]->(n) RETURN COUNT(n)"
                curs_time = timeit.timeit(lambda: client.execute_query(query, database_=config['name']), number=QUERY_RUNS)
                results.append((table_name, "S0", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                print(table_name, "S0")

                for recur in MAX_RECUR:
                    query = "MATCH (n:Node)-[x*0.." + str(recur-1) + "]->(o:Node) WHERE n.table = '" + table_name + "' AND NOT ()-[]->(n) RETURN COUNT(o)"
                    curs_time = timeit.timeit(lambda: client.execute_query(query, database_=config['name']), number=QUERY_RUNS)
                    results.append((table_name, "S" + str(recur), compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                    print(table_name, "S"+str(recur))

                if type is DBType.INTEGER:
                    query = "MATCH (n:Node) WHERE n.table = '" + table_name + "' AND n.payload = " + str(random.randint(0, 65536)) + " RETURN COUNT(n)"
                    curs_time = timeit.timeit(lambda: client.execute_query(query, database_=config['name']), number=QUERY_RUNS)
                    results.append((table_name, "I1", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                    print(table_name, "I1")

                    query = "MATCH (n:Node) WHERE n.table = '" + table_name + "' AND toInteger(n.payload) < " + str(random.randint(0, 65536)) + " RETURN COUNT(n)"
                    curs_time = timeit.timeit(lambda: client.execute_query(query, database_=config['name']), number=QUERY_RUNS)
                    results.append((table_name, "I2", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                    print(table_name, "I2")

                    query = "MATCH (n:Node) WHERE n.table = '" + table_name + "' AND toInteger(n.payload) % " + str(random.randint(0, 65536)) + " = 0 RETURN COUNT(n)"
                    curs_time = timeit.timeit(lambda: client.execute_query(query, database_=config['name']), number=QUERY_RUNS)
                    results.append((table_name, "I3", compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                    print(table_name, "I3")
                else:
                    for c in range(1,C_MAX_LENGTH+1):
                        query = "MATCH (n:Node) WHERE n.table = '" + table_name + "' AND toString(n.payload) CONTAINS '" + ''.join(random.choices(string.ascii_letters + string.digits, k=c)) + "' RETURN COUNT(n)"
                        curs_time = timeit.timeit(lambda: client.execute_query(query, database_=config['name']), number=QUERY_RUNS)
                        results.append((table_name, "C1", c, compute_avg_query_time_ms(curs_time, QUERY_RUNS)))
                        print(table_name, "C1:", c)
        for line in results:
            print(line)