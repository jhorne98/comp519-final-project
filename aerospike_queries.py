import aerospike
from aerospike_helpers import expressions as exp
from aerospike_helpers.operations import operations
from aerospike import predicates as p
from aerospike import exception as ae_ex # type: ignore

user = 'dbbench'
passwd = 'Bd8EtstJXINw3yfzzA97'
dbname = 'relationalgraphbench'

config = {
    'hosts': [('localhost', 3000)]
}

def run_aerospike_queries():
    client = aerospike.client(config).connect()

    # Prepare queries and expressions

    # I1
    '''
    expr = exp.Let(
        exp.Def('bin', exp.IntBin('payload')),
        exp.GE(exp.Var('bin'), 2400)
    ).compile()
    query_policy = {'expressions': expr}
    '''

    i1_query = client.query(dbname, 'onek_integer')

    i1_query.where(p.equals('payload', 65535))

    '''
    def record_set(record):
        (key, meta, bins) = record
        print('{0} | {1}'.format(key[2], bins))

    i1_query.foreach(record_set)
    '''

    i1_query.apply('count', 'count_star')

    print(i1_query.results())

    client.close()

if __name__ == '__main__':
    run_aerospike_queries()