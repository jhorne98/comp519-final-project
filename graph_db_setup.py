import sys
from enum import Enum
import random
import string

from neo4j import GraphDatabase
import pydgraph

from tables import GRAPH_DIST, DBLength, DBType

user = 'dbbench'
passwd = 'Bd8EtstJXINw3yfzzA97'
dbname = 'relationalgraphbench'

# Database config
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

# Memgraph also uses Neo4j's GraphDatabase driver, so operations for both can be generalized
def cypher_operations(config, db):
    with GraphDatabase.driver(config['uri'], auth=config['auth']) as client:
        client.verify_connectivity()

        for length in DBLength:
            for type in DBType:
                table_name = length.name.lower() + "_" + type.name.lower()
                created_nodes = [None]
                for idx in range(0, length.value):
                    parent = None if random.randint(1, GRAPH_DIST) == 1 else random.choice(created_nodes)
                    payload = None
                    if type is DBType.INTEGER:
                        payload = random.randint(1,65536)
                    if type is DBType.CHAR32K:
                        payload = ''.join(random.choices(string.ascii_letters + string.digits, k=32768))
                    if type is DBType.CHAR8K:
                        payload = ''.join(random.choices(string.ascii_letters + string.digits, k=8192))

                    new_node = client.execute_query(
                        "CREATE (n:Node {id: $id, payload: $payload, table: $table}) RETURN n.id",
                        id=idx,
                        payload=payload,
                        table=table_name,
                        database_=db,
                    )

                    new_node_id = new_node.records[0].data()['n.id']
                    created_nodes.append(new_node_id)

                    if parent is not None:
                        new_parent_rel = client.execute_query(
                            "MATCH (n1:Node),(n2:Node) WHERE n1.id=$id1 AND n1.table=$table AND n2.id=$id2 AND n2.table=$table CREATE (n1)-[r:PARENT_OF]->(n2)",
                            id1=parent,
                            id2=new_node_id,
                            table=table_name
                        )

def dgraph_operations():
    client_stub = pydgraph.DgraphClientStub(dgraph_config['uri'])
    client = pydgraph.DgraphClient(client_stub)

    for length in DBLength:
        for type in DBType:
            table_name = length.name.lower() + type.name.lower()
            created_nodes = [None]
            payload_type = 'int' if type is DBType.INTEGER else 'string'
            schema = table_name + '.id: int @index(int) .\n' \
                + table_name + '.payload: ' + payload_type + ' .\n' \
                + 'type ' + table_name + ' {\n' \
                + ' ' + table_name + '.id\n' \
                + ' ' + table_name + '.payload\n' \
                + '}\n'
            op = pydgraph.Operation(schema=schema)
            #op = pydgraph.Operation(drop_all=True)
            client.alter(op)
            for idx in range(0, length.value):
                parent = None if random.randint(1, GRAPH_DIST) == 1 else random.choice(created_nodes)
                payload = None
                if type is DBType.INTEGER:
                    payload = random.randint(1,65536)
                if type is DBType.CHAR32K:
                    payload = ''.join(random.choices(string.ascii_letters + string.digits, k=32768))
                if type is DBType.CHAR8K:
                    payload = ''.join(random.choices(string.ascii_letters + string.digits, k=8192))

                txn = client.txn()
                node_name = table_name + str(idx)
                try:
                    txn.mutate(set_nquads='_:' + node_name + ' <' + table_name + '.id> "' + str(idx) + '" .')
                    txn.mutate(set_nquads='_:' + node_name + ' <' + table_name + '.payload> "' + str(payload) + '" .')
                    created_nodes.append(node_name)

                    txn.commit()
                finally:
                    txn.discard()