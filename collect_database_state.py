from create_tables import *
import csv

if __name__ == "__main__":
    connection.setup(IP_ADDRESS, KEY_SPACE[0])

    statistics = [0] * 15
    warehouses = Warehouse.all()
    districts = District.all()
    customers = Customer.all()
    orders = Order.all()
    order_lines = OrderLine.all()
    stocks = Stock.all()
    for w in warehouses:
        statistics[0] += w.W_YTD
    for d in districts:
        statistics[1] += d.D_YTD
        statistics[2] += d.D_NEXT_O_ID
    for c in customers:
        statistics[3] += c.C_BALANCE
        statistics[4] += c.C_YTD_PAYMENT
        statistics[5] += c.C_PAYMENT_CNT
        statistics[6] += c.C_DELIVERY_CNT
    for o in orders:
        statistics[7] = max(statistics[7], o.O_ID)
        statistics[8] += o.O_OL_CNT
    for ol in order_lines:
        statistics[9] += ol.OL_AMOUNT
        statistics[10] += ol.OL_QUANTITY
    for s in stocks:
        statistics[11] += s.S_QUANTITY
        statistics[12] += s.S_YTD
        statistics[13] += s.S_ORDER_CNT
        statistics[14] += s.S_REMOTE_CNT

    with open('output/db-state.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow(statistics)
