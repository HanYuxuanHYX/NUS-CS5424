from create_tables import IP_ADDRESS, KEY_SPACE
from cassandra.cluster import Cluster

cluster = Cluster(IP_ADDRESS)
session = cluster.connect(KEY_SPACE[0])

session.execute(
    """
        CREATE MATERIALIZED VIEW order_by_customer
        AS SELECT "O_W_ID", "O_D_ID", "O_C_ID", "O_ID", "O_ENTRY_D", "O_CARRIER_ID"
        FROM "order"
        WHERE "O_W_ID" IS NOT NULL AND "O_D_ID" IS NOT NULL AND "O_C_ID" IS NOT NULL AND "O_ID" IS NOT NULL
        PRIMARY KEY (("O_W_ID", "O_D_ID", "O_C_ID"), "O_ID")
        WITH CLUSTERING ORDER BY ("O_ID" DESC);
    """
)

