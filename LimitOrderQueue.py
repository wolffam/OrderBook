from heapq import heappush, heappop


class OrderQueue:
    def __init__(self, order_type):
        self.pq = []
        self.order_dict = {}
        self.num_orders = 0
        self.REMOVED = '<removed-order_id>'
        if order_type.upper() == 'SELL':
            self.sell_negator = -1
        else:
            self.sell_negator = 1

    def add_limit_order(self, order_id, order_price, order_volume):
        'Add a new order_id or update the order_price of an existing order_id'
        order_price = self.sell_negator * order_price
        if order_id in self.order_dict:
            self.remove_limit_order(order_id)
        entry = [order_price, order_id, order_volume]
        self.order_dict[order_id] = entry
        heappush(self.pq, entry)
        self.num_orders += 1

    def remove_limit_order(self, order_id):
        'Mark an existing order_id as REMOVED.  Raise KeyError if not found.'
        entry = self.order_dict.pop(order_id)
        entry[-2] = self.REMOVED
        self.num_orders -= 1

    def pop_limit_order(self):
        'Remove and return the lowest order_price order_id. Raise KeyError if empty.'
        while self.pq:
            order_price, order_id, order_volume = heappop(self.pq)
            if order_id is not self.REMOVED:
                del self.order_dict[order_id]
                self.num_orders -= 1
                return [order_price * self.sell_negator, order_id, order_volume]
            else:
                raise ValueError('You tried to "pop" a removed order!')
        raise ValueError('Queue is empty!')


'''
q_sell_order = OrderQueue('sell')
in_list = [[95.0, 5], [100.0, 5], [90.0, 5]]
for position, item in enumerate(in_list):
    q_sell_order.add_limit_order(position, item[0], item[1])
print(q_sell_order.num_orders)
print(q_sell_order.pop_limit_order())



q_buy_order = LimitOrderQueue('buy')
in_list = [[95.0, 5], [100.0, 5], [90.0, 5]]
for position, item in enumerate(in_list):
    q_buy_order.add_limit_order(position, item[0], item[1])
print(q_buy_order.pop_limit_order())

'''