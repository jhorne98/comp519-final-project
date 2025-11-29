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

mariadb_oqgraph_config = {
    'host': 'localhost',
    'port': 3306,
    'user': user,
    'password': passwd,
    'database': 'extensiongraphbench',
}

postgres_config = {
    'host': 'localhost',
    'port': 5432,
    'user': user,
    'password': passwd,
    'dbname': dbname,
}

apache_age_config = {
    'host': 'localhost',
    'port': 5432,
    'user': user,
    'password': passwd,
    'dbname': 'extensiongraphbench'
}

aerospike_config = {
    'hosts': [('localhost', 3000)]
}

memgraph_config = {
    'name': "memgraph",
    'uri': "bolt://localhost:7687",
    'auth': ("", "")
}

neo4j_config = {
    'name': "neo4j",
    'uri': "neo4j://localhost",
    'auth': ("neo4j", "Bd8EtstJXINw3yfzzA97")
}

dgraph_config = {
    'uri': "localhost:9080"
}