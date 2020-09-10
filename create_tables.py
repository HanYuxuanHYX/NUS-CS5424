import uuid
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from datetime import datetime
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model


class Warehouse(Model):
    W_ID = columns.Integer(primary_key=True)
    W_NAME = columns.Text(max_length=10)
    W_STREET1 = columns.Text(max_length=20)
    W_STREET2 = columns.Text(max_length=20)
    W_CITY = columns.Text(max_length=20)
    W_STATE = columns.Text(max_length=2)
    W_ZIP = columns.Text(max_length=9)
    W_TAX = columns.Decimal()
    W_YTD = columns.Decimal()


class District(Model):
    D_W_ID = columns.Integer()
    D_ID = columns.Integer(primary_key=True)
    D_NAME = columns.Text(max_length=10)
    D_STREET1 = columns.Text(max_length=20)
    D_STREET2 = columns.Text(max_length=20)
    D_CITY = columns.Text(max_length=20)
    D_STATE = columns.Text(max_length=2)
    D_ZIP = columns.Text(max_length=9)
    D_TAX = columns.Decimal()
    D_YTD = columns.Decimal()
    D_NEXT_O_ID = columns.Integer()


class Customer(Model):
    C_W_ID = columns.Integer()
    C_D_ID = columns.Integer()
    C_ID = columns.Integer(primary_key=True)
    C_FIRST = columns.Text(max_length=16)
    C_MIDDLE = columns.Text(max_length=2)
    C_LAST = columns.Text(max_length=16)
    C_STREET1 = columns.Text(max_length=20)
    C_STREET2 = columns.Text(max_length=20)
    C_CITY = columns.Text(max_length=20)
    C_STATE = columns.Text(max_length=2)
    C_ZIP = columns.Text(max_length=9)
    C_PHONE = columns.Text(max_length=16)
    C_SINCE = columns.DateTime()
    C_CREDIT = columns.Text(max_length=2)
    C_CREDIT_LIM = columns.Decimal()
    C_DISCOUNT = columns.Decimal()
    C_BALANCE = columns.Decimal()
    C_YTD_PAYMENT = columns.Float()
    C_PAYMENT_CNT = columns.Integer()
    C_DELIVERY_CNT = columns.Integer()
    C_DATA = columns.Text(max_length=500)


class Order(Model):
    O_W_ID = columns.Integer()
    O_D_ID = columns.Integer()
    O_C_ID = columns.Integer()
    O_ID = columns.Integer(primary_key=True)
    O_CARRIER_ID = columns.Integer()
    O_OL_CNT = columns.Decimal()
    O_ALL_LOCAL = columns.Integer()
    O_ENTRY_D = columns.DateTime()


class Item(Model):
    I_ID = columns.Integer(primary_key=True)
    I_NAME = columns.Text(max_length=24)
    I_PRICE = columns.Decimal()
    I_IM_ID = columns.Integer()
    I_DATA = columns.Text(max_length=50)


class OrderLine(Model):
    OL_W_ID = columns.Integer()
    OL_D_ID = columns.Integer()
    OL_O_ID = columns.Integer()
    OL_NUMBER = columns.Integer()
    OL_I_ID = columns.Integer()
    OL_DELIVERY_D = columns.DateTime()
    OL_AMOUNT = columns.Decimal()
    OL_SUPPLY_W_ID = columns.Integer()
    OL_QUANTITY = columns.Decimal()
    OL_DIST_INFO = columns.Text(max_length=24)


class Stock(Model):
    S_W_ID = columns.Integer()
    S_D_ID = columns.Integer()
    S_QUANTITY = columns.Decimal()
    S_YTD = columns.Decimal()
    S_ORDER_CNT = columns.Integer()
    S_REMOTE_CNT = columns.Integer()
    S_DIST_01 = columns.Text(max_length=24)
    S_DIST_02 = columns.Text(max_length=24)
    S_DIST_03 = columns.Text(max_length=24)
    S_DIST_04 = columns.Text(max_length=24)
    S_DIST_05 = columns.Text(max_length=24)
    S_DIST_06 = columns.Text(max_length=24)
    S_DIST_07 = columns.Text(max_length=24)
    S_DIST_08 = columns.Text(max_length=24)
    S_DIST_09 = columns.Text(max_length=24)
    S_DIST_10 = columns.Text(max_length=24)
    S_DATA = columns.Text(max_length=50)


# next, setup the connection to your cassandra server(s)...
# see http://datastax.github.io/python-driver/api/cassandra/cluster.html for options
# the list of hosts will be passed to create a Cluster() instance
if __name__ == '__main__':
    connection.setup(['127.0.0.1'], "cqlengine", protocol_version=3)
    sync_table(Warehouse)
    Warehouse.create(W_ID=1, W_NAME='name1', W_STREET1='street11', W_STREET2='street22', W_CITY='city1', W_STATE='ST',
                     W_ZIP='12345', W_TAX=23.1323, W_YTD=113400.2933)
    print Warehouse.objects.count()
