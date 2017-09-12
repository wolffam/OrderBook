from orderbook import OrderQueue

q = OrderQueue('q', 'buy')

in_list = [['stop', 'buy', 1, 25], ['stop', 'buy', 2, 25]]
for position, item in enumerate(in_list):
    q.add_stop_order(item[3], position, item[2])

print(q.order_dict)
print(q.pq)

print(q.pop_stop_order())
