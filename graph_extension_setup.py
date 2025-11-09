import sys
from enum import Enum
import random
import string

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import P
import mariadb
import psycopg2

from tables import GRAPH_DIST, DBLength, DBType, DBTypePostgres

user = 'dbbench'
passwd = 'Bd8EtstJXINw3yfzzA97'
dbname = 'extensiongraphbench'

# Database configs
aerospike_graph_config = {
    'host': 'localhost',
    'port': 8182,
}

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

def aerospike_graph_operations():
    cluster = DriverRemoteConnection("ws://{host}:{port}/gremlin".format(host=aerospike_graph_config['host'], port=aerospike_graph_config['port']), "g")
    g = traversal().with_remote(cluster)

    try:
        for length in DBLength:
            for type in DBType:
                table_name = length.name.lower() + type.name.lower()
                created_nodes = [None]
                for i in range(1, length.value+1):
                    parent = None if random.randint(1, GRAPH_DIST) == 1 else random.choice(created_nodes)
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

def maria_oqgraph_operations():
    conn = None
    cursor = None

    # Connect to the mariadb service
    try:
        conn = mariadb.connect(**mariadb_config)
        curs = conn.cursor()

        try:
            curs.execute("INSTALL SONAME 'ha_oqgraph'")
        except mariadb.Error as e:
            print(f"Error setting oqgraph: {e}")
            conn.rollback()

        # Create tables
        try:
            for length in DBLength:
                for type in DBType:
                    table_name = length.name.lower() + "_" + type.name.lower()
                    query = "CREATE TABLE IF NOT EXISTS " + table_name + "_backing" \
                        + "(origid INT NOT NULL," \
                        + "destid INT NOT NULL," \
                        + "payload " + type.value + " NOT NULL, " \
                        + "PRIMARY KEY (origid, destid)," \
                        + "KEY (destid))"
                    curs.execute(query)

                    query = "CREATE TABLE IF NOT EXISTS " + table_name + "_graph " \
                        + "ENGINE=OQGRAPH " \
                        + "data_table='" + table_name + "_backing' origid='origid' destid='destid'"
                    curs.execute(query)

            conn.commit()
        except mariadb.Error as e:
            print(f"Error creating table: {e}")
            conn.rollback()

        # Insert randomized graphs
        try:
            for length in DBLength:
                for type in DBType:
                    table_name = length.name.lower() + "_" + type.name.lower()
                    created_nodes = [0]
                    query = "INSERT INTO " + table_name + "_backing (origid, destid, payload) VALUES (?, ?, ?)"
                    for i in range(1, length.value+1):
                        parent = 0 if random.randint(1, GRAPH_DIST) == 1 else random.choice(created_nodes)
                        if type is DBType.INTEGER:
                            curs.execute(query, (parent, i, random.randint(1, 65536)))
                        elif type is DBType.CHAR32K:
                            curs.execute(query, (parent, i, ''.join(random.choices(string.ascii_letters + string.digits, k=32768))))
                        elif type is DBType.CHAR8K:
                            curs.execute(query, (parent, i, ''.join(random.choices(string.ascii_letters + string.digits, k=8192))))
                        created_nodes.append(i)
            conn.commit()

        except mariadb.Error as e:
            print(f"Error creating graph: {e}")
            conn.rollback()
        
    except mariadb.Error as e:
        print(f"Error occured in MariaDB connection: {e}")
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def apache_age_operations():
    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**postgres_config)
        curs = conn.cursor()

        curs.execute("CREATE EXTENSION age")
        curs.execute("LOAD 'age'")
        curs.execute("SET search_path=ag_catalog, '$user', public")
        conn.commit()

        # Create tables
        try:
            for length in DBLength:
                for type in DBTypePostgres:
                    table_name = length.name.lower() + "_" + type.name.lower()
                    # Create graph table if not exists
                    curs.execute("SELECT graph_exists('" + table_name + "')")
                    if curs.fetchone()[0] == 'true':
                        continue
                    curs.execute("SELECT create_graph('" + table_name + "')")
                    conn.commit()
        except (Exception, psycopg2.Error) as e:
            print(f"Error creating table: {e}")
            conn.rollback()

        # Insert randomized graphs
        try:
            for length in DBLength:
                for type in DBTypePostgres:
                    table_name = length.name.lower() + "_" + type.name.lower()
                    created_nodes = [None]
                    #query = "INSERT INTO " + table_name + " (payload, parent) VALUES (%s, %s)"
                    for idx in range(1, length.value+1):
                        parent = None if random.randint(1, GRAPH_DIST) == 1 else random.choice(created_nodes)
                        payload = None

                        if type is DBTypePostgres.INTEGER:
                            payload = str(random.randint(1, 65536))
                        elif type is DBTypePostgres.CHAR32K:
                            payload = "'" + ''.join(random.choices(string.ascii_letters + string.digits, k=32768)) + "'"
                        elif type is DBTypePostgres.CHAR8K:
                            payload = "'" + ''.join(random.choices(string.ascii_letters + string.digits, k=8192)) + "'"

                        query = "SELECT * FROM cypher('" + table_name + "', $$ " \
                            + "CREATE (n:Node {id: " + str(idx) + ", payload: " + payload + "}) " \
                            + "$$) as (v agtype)"
                        curs.execute(query)
                        created_nodes.append(idx)

                        if parent is not None:
                            query = "SELECT * FROM cypher('" + table_name + "', $$ " \
                                + "MATCH (n1:Node), (n2:Node) " \
                                + "WHERE n1.id=" + str(parent) + " AND n2.id=" + str(idx) + " " \
                                + "CREATE (n1)-[e:PARENT_OF]->(n2) " \
                                + "RETURN e " \
                                + "$$) as (e agtype)"
                            curs.execute(query)
            conn.commit()

        except (Exception, psycopg2.Error) as e:
            print(f"Error creating graph: {e}")
            conn.rollback()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error occured in PostgreSQL connection: {e}")
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()