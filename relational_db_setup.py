import sys
from enum import Enum
import random
import string

import mariadb
import psycopg2
import aerospike
from aerospike import exception as ae_ex

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

class DBTypePostgres(Enum):
    INTEGER = "INT"
    CHAR8K = "VARCHAR(8192)"
    CHAR32K = "TEXT"

# Database configs
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

def mariadb_operations():
    conn = None
    cursor = None

    # Connect to the mariadb service
    try:
        conn = mariadb.connect(**mariadb_config)
        curs = conn.cursor()

        # Create tables
        try:
            for length in DBLength:
                for type in DBType:
                    table_name = length.name.lower() + "_" + type.name.lower()
                    query = "CREATE TABLE IF NOT EXISTS " + table_name \
                        + "(id INT AUTO_INCREMENT PRIMARY KEY, " \
                        + "payload " + type.value + " NOT NULL, " \
                        + "parent INT)"
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
                    created_nodes = [None]
                    query = "INSERT INTO " + table_name + "(payload, parent) VALUES (?, ?)"
                    for i in range(0, length.value):
                        parent = None if random.randint(1, 10) == 1 else random.choice(created_nodes)
                        if type is DBType.INTEGER:
                            curs.execute(query, (random.randint(1, 65536), parent))
                        elif type is DBType.CHAR32K:
                            curs.execute(query, (''.join(random.choices(string.ascii_letters + string.digits, k=32768)), parent))
                        elif type is DBType.CHAR8K:
                            curs.execute(query, (''.join(random.choices(string.ascii_letters + string.digits, k=8192)), parent))
                        created_nodes.append(i+1)
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

def postgres_operations():
    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(**postgres_config)
        curs = conn.cursor()
        
        # Create tables
        try:
            for length in DBLength:
                for type in DBTypePostgres:
                    table_name = length.name.lower() + "_" + type.name.lower()
                    query = "CREATE TABLE IF NOT EXISTS " + table_name \
                        + "(id SERIAL PRIMARY KEY, " \
                        + "payload " + type.value + " NOT NULL, " \
                        + "parent INT)"
                    curs.execute(query)
                    conn.commit()
        except (Exception, psycopg2.Error) as e:
            print(f"Error creating table: {e}")
            conn.rollback()

        # Insert randomized graphs
        try:
            for length in DBLength:
                for type in DBTypePostgres:
                    table_name = length.name.lower() + "_" + type.name.lower()
                    print(table_name)
                    created_nodes = [None]
                    query = "INSERT INTO " + table_name + " (payload, parent) VALUES (%s, %s)"
                    for i in range(0, length.value):
                        parent = None if random.randint(1, 10) == 1 else random.choice(created_nodes)
                        if type is DBTypePostgres.INTEGER:
                            curs.execute(query, (random.randint(1, 65536), parent))
                        elif type is DBTypePostgres.CHAR32K:
                            curs.execute(query, (''.join(random.choices(string.ascii_letters + string.digits, k=32768)), parent))
                        elif type is DBTypePostgres.CHAR8K:
                            curs.execute(query, (''.join(random.choices(string.ascii_letters + string.digits, k=8192)), parent))
                        created_nodes.append(i+1)
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

def aerospike_operations():
    try:
        client = aerospike.client(aerospike_config).connect()
    except aerospike.exception.ClientError as e:
        print("Error occured in Aerospike connection: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    try:
        for length in DBLength:
            for name in DBType:
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
                    record = {'id':i, 'payload':payload, 'parent':parent}
                    client.put(('test', length.name + "_"+ name.name, 'id'+str(record['id'])), record)
                    created_nodes.append(i)
                try:
                    client.index_integer_create("test", length.name + "_" + name.name, "parent", "test_" + length.name + "_" + name.name + "_idx")
                except ae_ex.IndexFoundError:
                    pass

    except Exception as e:
        print("error: {0}".format(e), file=sys.stderr)

if __name__ == "__main__":
    #mariadb_operations()
    #postgres_operations()
    aerospike_operations()