import relational_db_setup
import graph_db_setup
import graph_extension_setup

import sys

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if 'mariadb' in sys.argv:
            relational_db_setup.mariadb_operations()
        if 'postgres' in sys.argv:
            relational_db_setup.postgres_operations()
        if 'aerospike' in sys.argv:
            relational_db_setup.aerospike_operations()
        if 'memgraph' in sys.argv:
            graph_db_setup.cypher_operations(graph_db_setup.memgraph_config, "memgraph")
        if 'neo4j' in sys.argv:
            graph_db_setup.cypher_operations(graph_db_setup.neo4j_config, "neo4j")
        if 'dgraph' in sys.argv:
            graph_db_setup.dgraph_operations()
        if 'aerospike_graph' in sys.argv:
            graph_extension_setup.aerospike_graph_operations()
        if 'oqgraph' in sys.argv:
            graph_extension_setup.maria_oqgraph_operations()
        if 'apache_age' in sys.argv:
            graph_extension_setup.apache_age_operations()