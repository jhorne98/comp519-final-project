import sys
from enum import Enum
import random
import string

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import P

user = 'dbbench'
passwd = 'Bd8EtstJXINw3yfzzA97'
dbname = 'relationalgraphbench'

# Database configs
aerospike_graph_config = {
    'host': 'localhost',
    'port': 8182,
}

# Table query configs
class DBLength(Enum):
    ONEK = 1000
    FIVEK = 5000
    TENK = 10000
    #HUNDREDK = 100000

class DBType(Enum):
    INTEGER = "INT"
    CHAR8K = "VARCHAR(8192)"
    CHAR32K = "TEXT(32768)"

class DBTypePostgres(Enum):
    INTEGER = "INT"
    CHAR8K = "VARCHAR(8192)"
    CHAR32K = "TEXT"

def aerospike_graph_operations():
    cluster = DriverRemoteConnection("ws://{host}:{port}/gremlin".format(host=aerospike_graph_config['host'], port=aerospike_graph_config['port']), "g")
    g = traversal().with_remote(cluster)

    try:
        for length in DBLength:
            for type in DBType:
                table_name = length.name.lower() + type.name.lower()
                created_nodes = [None]
                for i in range(1, length.value+1):
                    parent = None if random.randint(1, 10) == 1 else random.choice(created_nodes)
                    payload = None
                    if type is DBType.INTEGER:
                        payload = random.randint(1,65536)
                    if type is DBType.CHAR32K:
                        payload = ''.join(random.choices(string.ascii_letters + string.digits, k=32768))
                    if type is DBType.CHAR8K:
                        payload = ''.join(random.choices(string.ascii_letters + string.digits, k=8192))

                    node = g.add_v("Node").property("nodeId", str(i)).property("table", table_name).property("payload", payload).next()
                    created_nodes.append(node)

                    if parent is not None:
                        g.add_e("parent_of").from_(parent).to(node).iterate()

    except Exception as e:
        print("error: {0}".format(e), file=sys.stderr)
    
    if cluster:
        try:
            print("Closing Connection...")
            cluster.close()
        except Exception as e:
            print(f"Failed to Close Connection: {e}")

if __name__ == '__main__':
    aerospike_graph_operations()