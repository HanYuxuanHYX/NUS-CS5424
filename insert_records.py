import csv
from tqdm import tqdm
from datetime import datetime
from create_tables import *


def parse_date_time(datetime):
    return datetime.strptime(datetime, '%Y-%m-%d %H:%M:%S.%f')


if __name__ == '__main__':
    connection.setup(IP_ADDRESS, KEY_SPACE[0])

    with open('project-files/data-files/customer.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Customers'
        for row in tqdm(reader):
            row[12] = parse_date_time(row[12])
            args = dict(zip(Customer().keys(), row))
            Customer.create(**args)

    with open('project-files/data-files/district.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Districts'
        for row in tqdm(reader):
            args = dict(zip(District().keys(), row))
            District.create(**args)

    with open('project-files/data-files/item.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Items'
        for row in tqdm(reader):
            args = dict(zip(Item().keys(), row))
            Item.create(**args)

    with open('project-files/data-files/order.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Orders'
        for row in tqdm(reader):
            row[7] = parse_date_time(row[7])
            args = dict(zip(Order().keys(), row))
            Order.create(**args)

    with open('project-files/data-files/order-line.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Order-lines'
        for row in tqdm(reader):
            row[5] = parse_date_time(row[5])
            args = dict(zip(OrderLine().keys(), row))
            OrderLine.create(**args)

    with open('project-files/data-files/stock.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Stocks'
        for row in tqdm(reader):
            args = dict(zip(Stock().keys(), row))
            Stock.create(**args)

    with open('project-files/data-files/warehouse.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        print 'Loading Warehouses'
        for row in tqdm(reader):
            args = dict(zip(Warehouse().keys(), row))
            Warehouse.create(**args)
