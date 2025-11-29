import timeit
import sys

from neo4j import GraphDatabase

from tables import QUERY_RUNS, DBLength, DBType, DBTypePostgres
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
                results.append((table_name, "S0", curs_time/QUERY_RUNS))
                print(table_name, "S0")

                max_recursions = [4,128,256]

                for recur in max_recursions:
                    query = "MATCH (n:Node)-[x*0.." + str(recur) + "]->(o:Node) WHERE n.table = '" + table_name + "' AND NOT ()-[:PARENT_OF]->(n) RETURN COUNT(n)"
                    curs_time = timeit.timeit(lambda: client.execute_query(query, database_=config['name']), number=QUERY_RUNS)
                    results.append((table_name, "S" + str(recur), curs_time/QUERY_RUNS))
                    print(table_name, "S"+str(recur))

                if type is DBType.INTEGER:
                    query = "MATCH (n:Node) WHERE n.table = '" + table_name + "' AND n.payload = 65535 RETURN COUNT(n)"
                    curs_time = timeit.timeit(lambda: client.execute_query(query, database_=config['name']), number=QUERY_RUNS)
                    results.append((table_name, "I1", curs_time/QUERY_RUNS))
                    print(table_name, "I1")

                    query = "MATCH (n:Node) WHERE n.table = '" + table_name + "' AND toInteger(n.payload) < 32767 RETURN COUNT(n)"
                    curs_time = timeit.timeit(lambda: client.execute_query(query, database_=config['name']), number=QUERY_RUNS)
                    results.append((table_name, "I2", curs_time/QUERY_RUNS))
                    print(table_name, "I2")
                else:
                    query = "MATCH (n:Node) WHERE n.table = '" + table_name + "' AND toString(n.payload) CONTAINS 'asdf' RETURN COUNT(n)"
                    curs_time = timeit.timeit(lambda: client.execute_query(query, database_=config['name']), number=QUERY_RUNS)
                    results.append((table_name, "C1", curs_time/QUERY_RUNS))
                    print(table_name, "C1")
        for line in results:
            print(line)

if __name__ == '__main__':
    time_graph_queries(configs.neo4j_config)
    #time_graph_queries(configs.memgraph_config)
