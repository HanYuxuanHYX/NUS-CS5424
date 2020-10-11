from datetime import datetime
from create_tables import *


def new_order_transaction(w_id, d_id, c_id, num_items, item_num, supplier_warehouse, quantity):
    warehouse = Warehouse.filter(W_ID=w_id).get()
    district = District.filter(D_W_ID=w_id, D_ID=d_id).get()
    customer = Customer.filter(C_W_ID=w_id, C_D_ID=d_id, C_ID=c_id).get()

    # Processing steps

    n = district.D_NEXT_O_ID

    district.update(D_NEXT_O_ID=n + 1)

    entry_time = datetime.utcnow()
    local = all([w_id == i for i in supplier_warehouse])
    Order.create(O_ID=n, O_D_ID=d_id, O_W_ID=w_id, O_C_ID=c_id, O_ENTRY_D=entry_time,
                 O_OL_CNT=num_items, O_ALL_LOCAL=local)

    total_amount = 0

    for i in range(num_items):
        item = Item.filter(I_ID=item_num[i]).get()
        stock = Stock.filter(S_W_ID=w_id, S_I_ID=item_num[i]).get()
        adjusted_qty = stock.S_QUANTITY - quantity[i]
        if adjusted_qty < 10:
            adjusted_qty += 100
        stock.update(S_QUANTITY=adjusted_qty)
        stock.update(S_YTD=stock.S_YTD + quantity[i])
        stock.update(S_ORDER_CNT=stock.S_ORDER_CNT + 1)
        if supplier_warehouse[i] != w_id:
            stock.update(S_REMOTE_CNT=stock.S_REMOTE_CNT + 1)
        item_amount = quantity[i] * item.I_PRICE
        total_amount += item_amount
        d_id_string = str(d_id) if d_id >= 10 else '0' + str(d_id)
        dist_info = getattr(stock, 'S_DIST_' + d_id_string)
        OrderLine.create(OL_O_ID=n, OL_D_ID=d_id, OL_W_ID=w_id, OL_NUMBER=i, OL_I_ID=item_num[i],
                         OL_SUPPLY_W_ID=supplier_warehouse[i], OL_QUANTITY=quantity[i], OL_AMOUNT=item_amount,
                         OL_DIST_INFO=dist_info)
        total_amount = total_amount * (1 + district.D_TAX + warehouse.W_TAX) * (1 - customer.C_DISCOUNT)

    # Output

    print 'customer identifier:', w_id, d_id, c_id, ', last name:', customer.C_LAST, ', credit:', customer.C_CREDIT, ', discount:', customer.C_DISCOUNT
    print 'warehouse tax rate:', warehouse.W_TAX, ', district tax rate:', district.D_TAX
    print 'order number:', n, ', entry date:', entry_time
    print 'number of items:', num_items, ', total amount:', total_amount
    for i in range(num_items):
        item = Item.filter(I_ID=item_num[i]).get()
        stock = Stock.filter(S_W_ID=w_id, S_I_ID=item_num[i]).get()
        orderLine = OrderLine.filter(OL_O_ID=n, OL_D_ID=d_id, OL_W_ID=w_id, OL_NUMBER=i).get()
        print 'item number:', item_num[i], ', item name:', item.I_NAME, ', supplier warehouse:', supplier_warehouse[
            i], ', quantity:', quantity[i], ', amount:', orderLine.OL_AMOUNT, ', stock quantity:', stock.S_QUANTITY


def popular_item_transaction(w_id, d_id, last):
    # Processing steps

    orders = Order.filter(O_W_ID=w_id, O_D_ID=d_id)[-last:]
    popular_list = []
    for order in orders:
        order_lines = OrderLine.filter(OL_W_ID=w_id, OL_D_ID=d_id, OL_O_ID=order.O_ID)
        max_qty = 0
        popular_ols = []
        for order_line in order_lines:
            if order_line.OL_QUANTITY > max_qty:
                max_qty = order_line.OL_QUANTITY
                popular_ols = [order_line.OL_NUMBER]
            elif order_line.OL_QUANTITY == max_qty:
                popular_ols.append(order_line.OL_NUMBER)
        popular_list.append(popular_ols)

    # Output

    print 'district identifier:', w_id, d_id
    print 'number of last orders to be examined:', last
    popular_items = set()
    for order, popular_ols in zip(orders, popular_list):
        print 'order number:', order.O_ID, ', entry date and time:', order.O_ENTRY_D
        customer = Customer.filter(C_W_ID=w_id, C_D_ID=d_id, C_ID=order.O_C_ID).get()
        print 'name of the customer who placed this order:', customer.C_FIRST, customer.C_MIDDLE, customer.C_LAST
        for popular_ol in popular_ols:
            order_line = OrderLine.filter(OL_W_ID=w_id, OL_D_ID=d_id, OL_O_ID=order.O_ID, OL_NUMBER=popular_ol).get()
            item = Item.filter(I_ID=order_line.OL_I_ID).get()
            print '\titem name:', item.I_NAME, ', quantity ordered:', order_line.OL_QUANTITY
            popular_items.add(item)
    for popular_item in popular_items:
        print 'item name:', popular_item.I_NAME
        count = 0
        for order in orders:
            order_lines = OrderLine.filter(OL_W_ID=w_id, OL_D_ID=d_id, OL_O_ID=order.O_ID)
            for order_line in order_lines:
                if order_line.OL_I_ID == popular_item.I_ID:
                    count += 1
                    break
        print 'percentage of orders that contain this item:', (count / last) * 100


if __name__ == '__main__':
    connection.setup(IP_ADDRESS, KEY_SPACE[0])
    new_order_transaction(1, 1, 1279, 2, [68195, 26567], [1, 1], [1, 5])
