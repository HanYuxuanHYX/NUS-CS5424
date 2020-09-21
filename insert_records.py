import csv

from create_tables import *

if __name__ == '__main__':
    connection.setup(IP_ADDRESS, KEY_SPACE)

    with open('project-files/data-files/customer.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            args = dict(zip(Customer().keys(), row))
            Customer.create(**args)

    with open('project-files/data-files/district.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            args = dict(zip(District().keys(), row))
            Customer.create(**args)

    with open('project-files/data-files/item.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            args = dict(zip(Item().keys(), row))
            Customer.create(**args)

    with open('project-files/data-files/order.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            args = dict(zip(Order().keys(), row))
            Customer.create(**args)

    with open('project-files/data-files/order-line.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            args = dict(zip(OrderLine().keys(), row))
            Customer.create(**args)

    with open('project-files/data-files/stock.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            args = dict(zip(Stock().keys(), row))
            Customer.create(**args)

    with open('project-files/data-files/warehouse.csv') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        for row in reader:
            args = dict(zip(Warehouse().keys(), row))
            Customer.create(**args)

