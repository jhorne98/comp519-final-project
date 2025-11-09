import relational_db_setup
import graph_db_setup
import graph_extension_setup

if __name__ == '__main__':
    #relational_db_setup.mariadb_operations()
    #relational_db_setup.postgres_operations()
    #relational_db_setup.aerospike_operations()
    #graph_db_setup.cypher_operations(graph_db_setup.memgraph_config, "memgraph")
    graph_db_setup.cypher_operations(graph_db_setup.neo4j_config, "neo4j")
    #graph_db_setup.dgraph_operations()
    #graph_extension_setup.aerospike_graph_operations()
    #graph_extension_setup.maria_oqgraph_operations()
    #graph_extension_setup.apache_age_operations()