from heapq import heappush, heappop
import logging
import sys


class OrderBook:
    def __init__(self):
        self.sell_queue = OrderQueue('sell_queue', 'sell')
        self.buy_queue = OrderQueue('buy_queue', 'buy')
        self.sell_stop_queue = StopOrderQueue('sell_stop_queue', 'sell')
        self.buy_stop_queue = StopOrderQueue('buy_stop_queue', 'buy')
        self.queue_list = [self.buy_queue, self.sell_queue, self.buy_stop_queue, self.sell_stop_queue]
        self.trade_book = TradeBook()

    def order_sorter(self, order_in):
        """The initial piping to decided how to handle the order"""
        logging.info('  PROCESSING ORDER NUMBER {}'.format(order_in.position))
        if order_in.type.upper() == 'LIMIT':
            self.limit_order_processor(order_in)
        elif order_in.type.upper() == 'MARKET':
            self.market_order_processor(order_in)
        elif order_in.type.upper() == 'STOP':
            self.stop_order_processor(order_in)
        elif order_in.type.upper() == 'CANCEL':
            self.cancel_order_processor(order_in.order_to_cancel)
        else:
            raise ValueError('Bad Inputs!')

    def limit_order_processor(self, order_in):
        """Processes limit orders handling leftover orders appropriately"""
        if order_in.side.upper() == 'SELL':
            # Add order to book if the price is above current highest bid
            if self.buy_queue.num_orders == 0 or self.buy_queue.extreme_price() < order_in.price:
                self.sell_queue.add_order(order_in.price, order_in.position, order_in.volume)
            else:
                while order_in.volume_to_trade > 0 and self.buy_queue.extreme_price() >= order_in.price:
                    if self.buy_queue.num_orders == 0:
                        # Add leftover shares of order to relevant queue
                        self.sell_queue.add_order(order_in.price, order_in.position, order_in.volume_to_trade)
                        return None
                    [buyer_price, buyer_number, buyer_volume] = self.buy_queue.pop_order()
                    # How many shares were traded?
                    trade_volume = self.shares_traded(order_in, buyer_price, buyer_number, buyer_volume)
                    self.trade_book.create_trade(buyer_price, trade_volume, order_in.position, buyer_number)
                    order_in.volume_to_trade = order_in.volume_to_trade - trade_volume
                    if self.buy_queue.num_orders == 0:
                        self.stop_trigger()
                        return None
                    # Add leftover shares from order to queue
                    if order_in.volume_to_trade != 0 and order_in.price > self.buy_queue.extreme_price():
                        self.sell_queue.add_order(order_in.price, order_in.position, order_in.volume_to_trade)
                        order_in.volume_to_trade = 0
                        self.stop_trigger()
        elif order_in.side.upper() == 'BUY':
            # Add order to book if the price is below current highest ask
            if self.sell_queue.num_orders == 0 or self.sell_queue.extreme_price() > order_in.price:
                self.buy_queue.add_order(order_in.price, order_in.position, order_in.volume)
            else:
                while order_in.volume_to_trade > 0 and self.sell_queue.extreme_price() <= order_in.price:
                    if self.sell_queue.num_orders == 0:
                        self.buy_queue.add_order(order_in.price, order_in.position, order_in.volume_to_trade)
                        return None
                    [seller_price, seller_number, seller_volume] = self.sell_queue.pop_order()
                    trade_volume = self.shares_traded(order_in, seller_price, seller_number, seller_volume)
                    order_in.volume_to_trade = order_in.volume_to_trade - trade_volume
                    self.trade_book.create_trade(seller_price, trade_volume, order_in.position, seller_number)
                    if self.sell_queue.num_orders == 0:
                        self.stop_trigger()
                        return None
                    if order_in.volume_to_trade != 0 and order_in.price < self.sell_queue.extreme_price():
                        self.buy_queue.add_order(order_in.price, order_in.position, order_in.volume_to_trade)
                        order_in.volume_to_trade = 0
                        self.stop_trigger()
        else:
            raise ValueError('Bad Inputs!')

    def shares_traded(self, order_in, price, number, vol):
        """Returns the volume of the trade"""
        if order_in.side.upper() == 'BUY':
            q = self.sell_queue
        else:
            q = self.buy_queue
        if vol > order_in.volume_to_trade:
            trade_volume = order_in.volume_to_trade
            # Add back leftover bid/ask order
            q.add_order(price, number, vol - order_in.volume_to_trade)
        else:
            trade_volume = vol
        return trade_volume

    def cancel_order_processor(self, order_num):
        """Checks all queues for the order to cancel"""
        for q in self.queue_list:
            try:
                q.remove_order(order_num)
                logging.info('  CANCELLED order number: {}'.format(order_num))
                return None
            except KeyError:
                logging.warning(' ORDER TO CANCEL NOT FOUND IN QUEUE')
                continue

    def stop_order_processor(self, order_in):
        """Simply adds a stop order to its appropriate queue"""
        if order_in.side.upper() == 'SELL':
            self.sell_stop_queue.add_order(order_in.price, order_in.position, order_in.volume)
        elif order_in.side.upper() == 'BUY':
            self.buy_stop_queue.add_order(order_in.price, order_in.position, order_in.volume)
        else:
            raise ValueError('Bad Inputs!')

    def market_order_processor(self, order_in):
        """Processes market orders handling leftover orders approriately"""
        if order_in.side.upper() == 'BUY':
            q = self.sell_queue
        else:
            q = self.buy_queue
        # Check for any orders on the queue
        if q.num_orders == 0:
            logging.warning(' No orders in {}...Cannot execute market order!'.format(q.name))
            return None
        # Process market order until entire volume has been filled
        if order_in.volume_to_trade == q.extreme_volume():
            # Market order and extreme order on the queue have the same volume
            [trade_price, buyer_number, trade_volume] = q.pop_order()
            self.trade_book.create_trade(trade_price, trade_volume, order_in.position, buyer_number)
            self.stop_trigger()
        elif order_in.volume_to_trade < q.extreme_volume():
            # Partial filling of an order in the queue -- pop extreme order and then add back modified order
            order_vol_remaining = q.extreme_volume() - order_in.volume_to_trade
            [trade_price, buyer_number, _order_vol_orig] = q.pop_order()
            self.trade_book.create_trade(trade_price, order_in.volume_to_trade, order_in.position, buyer_number)
            q.add_order(trade_price, buyer_number, order_vol_remaining)
            self.stop_trigger()
        else:
            # Multiple orders from the queue needed to satisfy the market order
            while order_in.volume_to_trade > 0:
                if q.num_orders == 0:
                    return None
                [q_price, q_number, q_volume] = q.pop_order()
                if q_volume < order_in.volume_to_trade:
                    trade_volume = q_volume
                else:
                    trade_volume = order_in.volume_to_trade
                    q.add_order(q_price, q_number, q_volume - trade_volume)
                order_in.volume_to_trade = order_in.volume_to_trade - trade_volume
                self.trade_book.create_trade(q_price, trade_volume, order_in.position, q_number)
                #self.stop_trigger()

    def stop_trigger(self):
        """"Checks if any stop order needs to be triggered"""
        while True:
            if self.find_prev_trade():
                previous_trade = self.find_prev_trade()
            else:
                return None
            # Check if both stop queues have relevant orders
            trade_executed = self.stop_both_checker(previous_trade)
            logging.info(trade_executed)
            if trade_executed == 0:
                # Check if both stop queues are empty
                if self.buy_stop_queue.num_orders == 0 and self.sell_stop_queue.num_orders == 0:
                    return None
                elif self.buy_stop_queue.num_orders > 0:
                    if self.buy_stop_queue.extreme_price() <= previous_trade.price:
                        [_price, stop_number, stop_volume] = self.stop_finder('buy')
                        stop_order = Order(stop_number, ['Market', 'BUY', stop_volume, 0.0])
                        self.market_order_processor(stop_order)
                    else:
                        return None
                elif self.sell_stop_queue.num_orders > 0:
                    if self.sell_stop_queue.extreme_price() >= previous_trade.price:
                        [_price, stop_number, stop_volume] = self.stop_finder('sell')
                        stop_order = Order(stop_number, ['Market', 'SELL', stop_volume, 0.0])
                        self.market_order_processor(stop_order)
                    else:
                        return None
                else:
                    return None
            else:
                continue

    def stop_both_checker(self, prev_trade):
        """Used to handle situations where there are stop orders in both queues"""
        # Check if any stop will need to be triggered
        trade_executed = 1
        # Exit if either queue is empty
        if self.buy_stop_queue.num_orders == 0 or self.sell_stop_queue.num_orders == 0:
            trade_executed = 0
            return trade_executed
        # Exit if conditions are not correct
        elif self.buy_stop_queue.extreme_price() > prev_trade.price > self.sell_stop_queue.extreme_price():
            trade_executed = 0
            return trade_executed
        elif self.buy_stop_queue.extreme_price() == self.sell_stop_queue.extreme_price():
            # Need to compare order numbers
            [buy_price, buy_number, buy_volume] = self.stop_finder('BUY')
            [sell_price, sell_number, sell_volume] = self.stop_finder('SELL')
            if buy_number < sell_number:
                # Execute the buy stop trade
                stop_order = Order(buy_number, ['Market', 'BUY', buy_volume, 0.0])
                self.sell_stop_queue.add_order(sell_price, sell_number, sell_volume)
                self.market_order_processor(stop_order)
            else:
                # Execute the sell stop trade
                stop_order = Order(sell_number, ['Market', 'SELL', sell_volume, 0.0])
                self.buy_stop_queue.add_order(buy_price, buy_number, buy_volume)
                self.market_order_processor(stop_order)
        elif prev_trade.price >= self.buy_stop_queue.extreme_price():
            [_stop_price, stop_number, stop_volume] = self.stop_finder('BUY')
            stop_order = Order(stop_number, ['Market', 'BUY', stop_volume, 0.0])
            self.market_order_processor(stop_order)
        elif prev_trade.price <= self.sell_stop_queue.extreme_price():
            [_stop_price, stop_number, stop_volume] = self.stop_finder('SELL')
            stop_order = Order(stop_number, ['Market', 'SELL', stop_volume, 0.0])
            self.market_order_processor(stop_order)
        else:
            logging.warning('  Unintended stop order details!!!')
        return trade_executed

    def stop_finder(self, side):
        """Loop through triggered stop orders and find the earliest"""
        prev_trade = self.find_prev_trade()
        stop_dict = {}
        loop_num = 1
        if side.upper() == 'BUY':
            q = self.buy_stop_queue
            price1 = q.extreme_price()
            price2 = prev_trade.price
        else:
            q = self.sell_stop_queue
            price1 = prev_trade.price
            price2 = q.extreme_price()
        max_iter = q.num_orders
        while price1 <= price2:
            [stop_price, stop_number, stop_volume] = q.pop_order()
            stop_dict[stop_number] = [stop_price, stop_number, stop_volume]
            if loop_num >= max_iter:
                break
            else:
                loop_num += 1
        [stop_price, stop_number, stop_volume] = stop_dict.pop(min(stop_dict))
        for key in stop_dict:
            q.add_order(stop_dict[key][0], key, stop_dict[key][2])
        return [stop_price, stop_number, stop_volume]

    def find_prev_trade(self):
        if not self.trade_book.trade_list:
            return None
        prev_trade = self.trade_book.trade_list[-1]
        return prev_trade


class Order:
    def __init__(self, pos, in_list):
        self.position = pos
        self.type = in_list[0]
        self.side = in_list[1]
        if self.type.upper() == 'CANCEL':
            self.order_to_cancel = in_list[2]
        else:
            self.volume = in_list[2]
            self.volume_to_trade = in_list[2]
        self.price = in_list[3]


class OrderQueue:
    def __init__(self, name, order_type):
        self.name = name
        self.pq = []
        self.order_dict = {}
        self.num_orders = 0
        self.REMOVED = '<removed-order_id>'
        if order_type.upper() == 'SELL':
            self.sell_negator = 1
        else:
            self.sell_negator = -1

    def add_order(self, order_price, order_id, order_volume):
        """Add a new order_id or update the order_price of an existing order_id"""
        logging.info(
            '  ADDING {} shares at ${} with #{} into {}'.format(order_volume, order_price, order_id,
                                                                self.name))
        order_price = self.sell_negator * order_price
        if order_id in self.order_dict:
            self.remove_order(order_id)
        entry = [order_price, order_id, order_volume]
        self.order_dict[order_id] = entry
        heappush(self.pq, entry)
        self.num_orders += 1

    def remove_order(self, order_id):
        """Mark an existing order_id as REMOVED.  Raise KeyError if not found."""
        entry = self.order_dict.pop(order_id)
        entry[-2] = self.REMOVED
        self.num_orders -= 1
        # Check if removed order is next to pop
        if self.pq[0][1] == self.REMOVED:
            heappop(self.pq)

    def pop_order(self):
        """Pop sorted by order_price then order_id. Raise KeyError if empty."""
        while self.pq:
            order_price, order_id, order_volume = heappop(self.pq)
            if order_id is not self.REMOVED:
                del self.order_dict[order_id]
                self.num_orders -= 1
                logging.info('  REMOVING {} shares at ${} with #{} from {}'.format(order_volume,
                                                                                   order_price * self.sell_negator,
                                                                                   order_id, self.name))
                return [order_price * self.sell_negator, order_id, order_volume]
        raise ValueError('Queue is empty!')

    def extreme_price(self):
        """Return the relevant extreme price -- Highest bid or lowest ask"""
        while self.pq[0][1] == self.REMOVED:
            heappop(self.pq)
        order = self.pq[0]
        return order[0] * self.sell_negator

    def extreme_volume(self):
        """Return the relevant extreme volume -- Highest bid or lowest ask"""
        while self.pq[0][1] == self.REMOVED:
            heappop(self.pq)
        order = self.pq[0]
        return order[2]


class StopOrderQueue(OrderQueue):
    def __init__(self, name, order_type):
        """Define subclass of OrderQueue to handle priority queue properly"""
        OrderQueue.__init__(self, name, order_type)
        self.name = name
        if order_type.upper() == 'SELL':
            self.sell_negator = -1
        else:
            self.sell_negator = 1


class Trade:
    def __init__(self, trade_price, trade_vol, order_in_num, order_q_num):
        self.shares = trade_vol
        self.price = trade_price
        self.order_in_num = order_in_num
        self.order_q_num = order_q_num


class TradeBook:
    def __init__(self):
        self.trade_list = []

    def create_trade(self, trade_price, trade_vol, order_in_num, order_q_num):
        """Creates a trade object and adds it to the trade list"""
        trade = Trade(trade_price, trade_vol, order_in_num, order_q_num)
        self.trade_list.append(trade)
        logging.error('match %d %d %d %.2f' % (trade.order_in_num, trade.order_q_num, trade.shares, trade.price))
