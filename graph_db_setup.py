import sys
from enum import Enum
import random
import string

from neo4j import GraphDatabase

user = 'dbbench'
passwd = 'Bd8EtstJXINw3yfzzA97'
dbname = 'relationalgraphbench'

# Table query configs
class DBLength(Enum):
    ONEK = 1000
    #FIVEK = 5000
    #TENK = 10000
    #HUNDREDK = 100000

class DBType(Enum):
    INTEGER = "INT"
    CHAR8K = "VARCHAR(8192)"
    CHAR32K = "TEXT(32768)"

# Database config
memgraph_config = {
    'uri': "bolt://localhost:7687",
    'auth': ("", "")
}

def memgraph_operations():
    with GraphDatabase.driver(memgraph_config['uri'], auth=memgraph_config['auth']) as client:
        client.verify_connectivity()

        for length in DBLength:
            for type in DBType:
                table_name = length.name.lower() + "_" + type.name.lower()
                created_nodes = [None]
                for idx in range(0, length.value):
                    parent = None if random.randint(1, 10) == 1 else random.choice(created_nodes)
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
                        database_="memgraph",
                    )

                    new_node_id = new_node.records[0].data()['n.id']
                    created_nodes.append(new_node_id)

                    if parent is not None:
                        new_parent_rel = client.execute_query(
                            "MATCH (n1:Node),(n2:Node) WHERE n1.id=$id1 AND n1.table=$table AND n2.id=$id2 AND n2.table=$table CREATE (n1)-[r:PARENT_OF]->(n2)",
                            id1=new_node_id,
                            id2=parent,
                            table=table_name
                        )

if __name__ == '__main__':
    memgraph_operations()