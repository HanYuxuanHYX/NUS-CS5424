import sys
import time
import csv
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra import ConsistencyLevel
from queries import *


def run_xact_file(exp_n, client_n):
    xact_times = []
    path = 'project-files/xact-files/' + client_n + '.txt'

    with open(path) as xact_file:
        while True:
            line = xact_file.readline()
            if not line:
                break
            line = line.split(',')
            xact_type = line[0]
            xact_params = [eval(param) for param in line[1:]]
            start_time = time.time()
            if xact_type == 'N':
                c_id = xact_params[0]
                w_id = xact_params[1]
                d_id = xact_params[2]
                num_items = xact_params[3]
                item_num = []
                supplier_warehouse = []
                quantity = []
                for _ in range(num_items):
                    line = xact_file.readline().split(',')
                    item_params = [eval(param) for param in line]
                    item_num.append(item_params[0])
                    supplier_warehouse.append(item_params[1])
                    quantity.append(item_params[2])
                new_order_transaction(w_id, d_id, c_id, num_items, item_num, supplier_warehouse, quantity)
            if xact_type == 'P':
                payment_transaction(*xact_params)
            if xact_type == 'D':
                delivery_transaction(*xact_params)
            if xact_type == 'O':
                order_status_transaction(*xact_params)
            if xact_type == 'S':
                stock_level_transaction(*xact_params)
            if xact_type == 'I':
                popular_item_transaction(*xact_params)
            if xact_type == 'T':
                top_balance_transaction()
            if xact_type == 'R':
                related_customer_transaction(*xact_params)
            xact_times.append(time.time() - start_time)

    n_xact = len(xact_times)
    total_time = sum(xact_times)
    throughput = n_xact / total_time
    average_latency = 1000 / throughput
    sorted_times = sorted(xact_times)
    median_latency = sorted_times[n_xact % 2]
    percentile_95 = sorted_times[int(n_xact * 0.95)]
    percentile_99 = sorted_times[int(n_xact * 0.99)]
    with open('clients.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow(
            [exp_n, client_n, n_xact, total_time, throughput, average_latency, median_latency, percentile_95,
             percentile_99])


if __name__ == "__main__":
    experiment_number = sys.argv[1]
    client_number = sys.argv[2]

    if experiment_number == 1 or experiment_number == 3:
        profile = ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM,
                                   serial_consistency_level=ConsistencyLevel.QUORUM)
    else:
        profile = ExecutionProfile(consistency_level=ConsistencyLevel.ALL,
                                   serial_consistency_level=ConsistencyLevel.ONE)
    cluster = Cluster(IP_ADDRESS, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect(KEY_SPACE[0])
    connection.register_connection('default', session=session)

    run_xact_file(experiment_number, client_number)
