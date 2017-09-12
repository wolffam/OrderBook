# OrderBook
Stock Market Order Book Simulator

This system can handle buy/sell limit and market orders as well as stop and stop limit orders. Orders may be cancelled if they are sitting on the bid/ask.  The order book uses a heap queue to handle priority of orders.

## LimitOrderQueue
Creates the underlying data type for the bid/ask queues.  There are also stop limit queues created.
## OrderBook
Contains the additional order types and implements the OrderQueue class to handle incoming and outgoing orders
## orderTestCases
The high level code that takes a text file of orders and info and executes the transactions
