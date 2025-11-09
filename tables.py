from enum import Enum

GRAPH_DIST = 20

# Table query configs
class DBLength(Enum):
    ONEK = 1000
    FIVEK = 5000
    #TENK = 10000
    #HUNDREDK = 100000

class DBType(Enum):
    INTEGER = "INT"
    CHAR8K = "VARCHAR(8192)"
    CHAR32K = "TEXT(32768)"

class DBTypePostgres(Enum):
    INTEGER = "INT"
    CHAR8K = "VARCHAR(8192)"
    CHAR32K = "TEXT"