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
    FIVEK = 5000
    TENK = 10000
    HUNDREDK = 100000

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

        # Create a user in the database
        records, summary, keys = client.execute_query(
            "CREATE (u:User {name: $name, password: $password}) RETURN u.name AS name;",
            name="John",
            password="pass",
            database_="memgraph",
        )

        # Get the result
        for record in records:
            print(record["name"])

        # Print the query counters
        print(summary.counters)

        for length in DBLength:
            for type in DBType:
                table_name = length.name.lower() + "_" + type.name.lower()
                created_nodes = [None]
                for i in range(0, length.value):
                    parent = None if random.randint(1, 10) == 1 else random.choice(created_nodes)
                    payload = None
                    if type is DBType.INTEGER:
                        payload = random.randint(1,65536)
                    if type is DBType.CHAR32K:
                        payload = ''.join(random.choices(string.ascii_letters + string.digits, k=32768))
                    if type is DBType.CHAR8K:
                        payload = ''.join(random.choices(string.ascii_letters + string.digits, k=8192))

if __name__ == '__main__':
    memgraph_operations()