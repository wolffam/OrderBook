from orderbook import OrderBook, Order
import logging
import sys


def main():
    logging.basicConfig(level=logging.ERROR)
    book = OrderBook()

    with open('stock_orders_bug5.txt') as f:
        content = f.readlines()
    # you may also want to remove whitespace characters like `\n` at the end of each line
    content = [x.strip().split() for x in content]
    for position, order_input in enumerate(content):
        order_input[2] = int(order_input[2])
        order_input[3] = float(order_input[3])
        order = Order(position + 1, order_input)
        # print(order.__dict__)
        book.order_sorter(order)
        prev = book.find_prev_trade()

if __name__ == "__main__":
    main()