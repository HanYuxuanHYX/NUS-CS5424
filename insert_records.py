from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory
from cassandra import ConsistencyLevel
from create_tables import *

import csv
import datetime
from tqdm import tqdm


def parse_date_time(time_str):
    return datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')


if __name__ == '__main__':

    profile = ExecutionProfile(
        consistency_level=ConsistencyLevel.THREE,
        request_timeout=30,
    )
    cluster = Cluster(IP_ADDRESS, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect(KEY_SPACE[0])
    set_session(session)

    with open('project-files/data-files/customer.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Customers'

        for row in tqdm(reader):
            keys = Customer().keys()
            fields = []
            values = []
            for i in range(len(keys)):
                if row[i] == 'null':
                    continue
                else:
                    fields.append(keys[i])
                    if isinstance(Customer._columns[keys[i]], columns.DateTime):
                        values.append(parse_date_time(row[i]))
                    else:
                        values.append(row[i])
            args = dict(zip(fields, values))
            Customer.create(**args)

    with open('project-files/data-files/district.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Districts'

        for row in tqdm(reader):
            keys = District().keys()
            fields = []
            values = []
            for i in range(len(keys)):
                if row[i] == 'null':
                    continue
                else:
                    fields.append(keys[i])
                    if isinstance(District._columns[keys[i]], columns.DateTime):
                        values.append(parse_date_time(row[i]))
                    else:
                        values.append(row[i])
            args = dict(zip(fields, values))
            District.create(**args)

    with open('project-files/data-files/item.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Items'

        for row in tqdm(reader):
            keys = Item().keys()
            fields = []
            values = []
            for i in range(len(keys)):
                if row[i] == 'null':
                    continue
                else:
                    fields.append(keys[i])
                    if isinstance(Item._columns[keys[i]], columns.DateTime):
                        values.append(parse_date_time(row[i]))
                    else:
                        values.append(row[i])
            args = dict(zip(fields, values))
            Item.create(**args)

    with open('project-files/data-files/order.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Orders'

        for row in tqdm(reader):
            keys = Order().keys()
            fields = []
            values = []
            for i in range(len(keys)):
                if row[i] == 'null':
                    continue
                else:
                    fields.append(keys[i])
                    if isinstance(Order._columns[keys[i]], columns.DateTime):
                        values.append(parse_date_time(row[i]))
                    else:
                        values.append(row[i])
            args = dict(zip(fields, values))
            Order.create(**args)

    with open('project-files/data-files/order-line.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Order-lines'

        for row in tqdm(reader):
            keys = OrderLine().keys()
            fields = []
            values = []
            for i in range(len(keys)):
                if row[i] == 'null':
                    continue
                else:
                    fields.append(keys[i])
                    if isinstance(OrderLine._columns[keys[i]], columns.DateTime):
                        values.append(parse_date_time(row[i]))
                    else:
                        values.append(row[i])
            args = dict(zip(fields, values))
            OrderLine.create(**args)

    with open('project-files/data-files/stock.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Stocks'

        for row in tqdm(reader):
            keys = Stock().keys()
            fields = []
            values = []
            for i in range(len(keys)):
                if row[i] == 'null':
                    continue
                else:
                    fields.append(keys[i])
                    if isinstance(Stock._columns[keys[i]], columns.DateTime):
                        values.append(parse_date_time(row[i]))
                    else:
                        values.append(row[i])
            args = dict(zip(fields, values))
            Stock.create(**args)

    with open('project-files/data-files/warehouse.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Warehouses'

        for row in tqdm(reader):
            keys = Warehouse().keys()
            fields = []
            values = []
            for i in range(len(keys)):
                if row[i] == 'null':
                    continue
                else:
                    fields.append(keys[i])
                    if isinstance(Warehouse._columns[keys[i]], columns.DateTime):
                        values.append(parse_date_time(row[i]))
                    else:
                        values.append(row[i])
            args = dict(zip(fields, values))
            Warehouse.create(**args)
