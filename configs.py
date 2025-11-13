user = 'dbbench'
passwd = 'Bd8EtstJXINw3yfzzA97'
dbname = 'relationalgraphbench'

mariadb_config = {
    'host': 'localhost',
    'port': 3306,
    'user': user,
    'password': passwd,
    'database': dbname,
}

postgres_config = {
    'host': 'localhost',
    'port': 5432,
    'user': user,
    'password': passwd,
    'dbname': dbname,
}

aerospike_config = {
    'hosts': [('localhost', 3000)]
}

memgraph_config = {
    'uri': "bolt://localhost:7687",
    'auth': ("", "")
}

neo4j_config = {
    'uri': "neo4j://localhost",
    'auth': ("neo4j", "Bd8EtstJXINw3yfzzA97")
}

dgraph_config = {
    'uri': "localhost:9080"
}