from create_tables import IP_ADDRESS, KEY_SPACE
from cassandra.cluster import Cluster

cluster = Cluster(IP_ADDRESS)
session = cluster.connect(KEY_SPACE[0])

session.execute(
    """
        CREATE MATERIALIZED VIEW customer_sort_by_balance
        AS SELECT "C_W_ID", "C_D_ID", "C_FIRST", "C_MIDDLE", "C_LAST", "C_BALANCE"
        FROM customer
        WHERE "C_W_ID" IS NOT NULL AND "C_D_ID" IS NOT NULL AND "C_ID" IS NOT NULL AND "C_BALANCE" IS NOT NULL
        PRIMARY KEY (("C_W_ID", "C_D_ID", "C_ID"), "C_BALANCE")
        WITH CLUSTERING ORDER BY ("C_BALANCE" DESC);
    """
)


