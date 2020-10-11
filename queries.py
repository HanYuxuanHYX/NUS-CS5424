from datetime import datetime
from create_tables import *


def new_order_transaction(w_id, d_id, c_id, num_items, item_num, supplier_warehouse, quantity):
    warehouse = Warehouse.objects.filter(W_ID=w_id).get()
    district = District.objects.filter(D_W_ID=w_id, D_ID=d_id).get()
    customer = Customer.objects.filter(C_W_ID=w_id, C_D_ID=d_id, C_ID=c_id).get()

    # Processing steps

    n = district.D_NEXT_O_ID

    district.D_NEXT_O_ID = n + 1

    entry_time = datetime.utcnow()
    local = all([w_id == i for i in supplier_warehouse])
    Order.create(O_ID=n, O_D_ID=d_id, O_W_ID=w_id, O_C_ID=c_id, O_ENTRY_D=entry_time,
                 O_OL_CNT=num_items, O_ALL_LOCAL=local)

    total_amount = 0

    for i in range(num_items):
        item = Item.objects.filter(I_ID=item_num[i]).get()
        stock = Stock.objects.filter(S_W_ID=w_id, S_I_ID=item_num[i]).get()
        adjusted_qty = stock.S_QUANTITY - quantity[i]
        if adjusted_qty < 10:
            adjusted_qty += 100
        stock.S_QUANTITY = adjusted_qty
        stock.S_YTD += quantity[i]
        stock.S_ORDER_CNT += 1
        if supplier_warehouse[i] != w_id:
            stock.S_REMOTE_CNT += 1
        item_amount = quantity[i] * item.I_PRICE
        total_amount += item_amount
        d_id_string = str(d_id) if d_id >= 10 else '0' + str(d_id)
        dist_info = getattr(stock, 'S_DIST' + d_id_string)
        OrderLine.create(OL_O_ID=n, OL_D_ID=d_id, OL_W_ID=w_id, OL_NUMBER=i, OL_I_ID=item_num[i],
                         OL_SUPPLY_W_ID=supplier_warehouse[i], OL_QUANTITY=quantity[i], OL_AMOUNT=item_amount,
                         OL_DIST_INFO=dist_info)
        total_amount = total_amount * (1 + district.D_TAX + warehouse.W_TAX) * (1 - customer.C_DISCOUNT)

    # Output

    print('customer identifier:', w_id, d_id, c_id, ', last name:', customer.C_LAST, ', credit:', customer.C_CREDIT,
          ', discount:', customer.C_DISCOUNT)
    print('warehouse tax rate:', warehouse.W_TAX, ', district tax rate:', district.D_TAX)
    print('order number:', n, ', entry date:', entry_time)
    print('number of items:', num_items, ', total amount:', total_amount)
    for i in range(num_items):
        item = Item.objects.filter(I_ID=item_num[i]).get()
        stock = Stock.objects.filter(S_W_ID=w_id, S_I_ID=item_num[i]).get()
        orderLine = OrderLine.objects.filter(OL_O_ID=n, OL_D_ID=d_id, OL_W_ID=w_id, OL_NUMBER=i).get()
        print('item number:', item_num[i], ', item name:', item.I_NAME, ', supplier warehouse:', supplier_warehouse[i],
              ', quantity:', quantity[i], ', amount:', orderLine.OL_AMOUNT, ', stock quantity:', stock.S_QUANTITY)


if __name__ == '__main__':
    new_order_transaction(1, 1, 1279, 2, [68195, 26567], [1, 1], [1, 5])
