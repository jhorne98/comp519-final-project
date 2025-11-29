import time_relational
import time_graph
import time_graph_extension

import configs

import sys

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if 'mariadb' in sys.argv:
            time_relational.time_mariadb_queries()
        if 'postgres' in sys.argv:
            time_relational.time_postgres_queries()

        if 'memgraph' in sys.argv:
            time_graph.time_graph_queries(configs.memgraph_config)
        if 'neo4j' in sys.argv:
            time_graph.time_graph_queries(configs.neo4j_config)

        if 'oqgraph' in sys.argv:
            time_graph_extension.time_mariadb_oqgraph_queries()
        if 'apache_age' in sys.argv:
            time_graph_extension.time_apache_age_queries()