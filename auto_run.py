from queries import *


def parse_xact_file(path):
    with open(path) as xact_file:
        while True:
            xact_params = xact_file.readline().split(',')
            xact_type = xact_params[0]

            if xact_type == 'N':
                c_id = xact_params[1]
                w_id = xact_params[2]
                d_id = xact_params[3]
                num_items = xact_params[4]
                item_num = []
                supplier_warehouse = []
                quantity = []
                for _ in range(int(num_items)):
                    item_params = xact_file.readline().split(',')
                    item_num.append(item_params[0])
                    supplier_warehouse.append(item_params[1])
                    quantity.append(item_params[2])
                new_order_transaction(w_id, d_id, c_id, num_items, item_num, supplier_warehouse, quantity)
            if xact_type == 'P':
                payment_transaction(*xact_params[1:])
            if xact_type == 'D':
                delivery_transaction(*xact_params[1:])
            if xact_type == 'O':
                order_status_transaction(*xact_params[1:])
            if xact_type == 'S':
                stock_level_transaction(*xact_params[1:])
            if xact_type == 'I':
                popular_item_transaction(*xact_params[1:])
            if xact_type == 'T':
                top_balance_transaction()
            if xact_type == 'R':
                related_customer_transaction(*xact_params[1:])


if __name__ == "__main__":
    connection.setup(IP_ADDRESS, KEY_SPACE[0])
    parse_xact_file('project-files/xact-files/1.txt')
