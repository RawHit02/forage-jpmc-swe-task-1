import csv
import http.server
import json
import operator
import re
import socketserver
import threading
from datetime import timedelta, datetime

import dateutil.parser


# from itertools import izip

# Configurations and constants...

# Rest of the code...

def read_csv():
    """ Read a CSV or order history into a list. """
    with open('test.csv', 'r') as f:
        for time, stock, side, order, size in csv.reader(f):
            yield dateutil.parser.parse(time), stock, side, float(order), int(size)


# Rest of the code...

class ThreadedHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """ Boilerplate class for a multithreaded HTTP Server, with working
        shutdown.
    """
    allow_reuse_address = True

    def shutdown(self):
        """ Override MRO to shutdown properly. """
        self.socket.close()
        super().shutdown()


def route(path):
    """ Decorator for a simple bottle-like web framework. Routes path to the
        decorated method, with the rest of the path as an argument.
    """

    def _route(f):
        setattr(f, '__route__', path)
        return f

    return _route


def read_params(path):
    """ Read query parameters into a dictionary if they are parseable,
        otherwise returns None.
    """
    query = path.split('?')
    if len(query) > 1:
        query = query[1].split('&')
        return dict(map(lambda x: x.split('='), query))


def get(req_handler, routes):
    """ Map a request to the appropriate route of a routes instance. """
    for name, handler in routes.__class__.__dict__.items():
        if hasattr(handler, "__route__"):
            if None != re.search(handler.__route__, req_handler.path):
                req_handler.send_response(200)
                req_handler.send_header('Content-Type', 'application/json')
                req_handler.send_header('Access-Control-Allow-Origin', '*')
                req_handler.end_headers()
                params = read_params(req_handler.path)
                data = json.dumps(handler(routes, params)) + '\n'
                req_handler.wfile.write(bytes(data, encoding='utf-8'))
                return


def run(routes, host='0.0.0.0', port=8080):
    """ Runs a class as a server whose methods have been decorated with
        @route.
    """

    class RequestHandler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args, **kwargs):
            pass

        def do_GET(self):
            get(self, routes)

    server = ThreadedHTTPServer((host, port), RequestHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    print('HTTP server started on port 8080')
    while True:
        from time import sleep
        sleep(1)
    server.shutdown()
    server.start()
    server.waitForThread()


# App and routes...

ops = {
    'buy': operator.le,
    'sell': operator.ge,
}
def order_book(data, book, stock):
    """ Maintains an order book given an iterator of (time, stock, side,
        order, size) tuples.  data is sorted by time.
    """
    for now, stock, side, price, size in data:
        if side == 'buy':
            book.setdefault(price, []).append((now, size))
        else:
            book.setdefault(price, []).append((now, -size))
        # Remove empty price levels from the book
        if not book[price]:
            del book[price]
        yield now, stock, side, price, size


class App:
    """ The trading game server application. """

    def __init__(self, order_book):
        self._book_1 = {}
        self._book_2 = {}
        self._data_1 = order_book(read_csv(), self._book_1, 'ABC')
        self._data_2 = order_book(read_csv(), self._book_2, 'DEF')
        self._rt_start = datetime.now()
        self._sim_start, _, _ = next(self._data_1)
        self._sim_start, _, _ = next(self._data_2)
        self._rt_since_sim = timedelta(0)

    @route(r'/getOrderBook1')
    def get_order_book_1(self, routes, params):
        return {k: len(v) for k, v in self._book_1.items()}

    @route(r'/getOrderBook2')
    def get_order_book_2(self, routes, params):
        return {k: len(v) for k, v in self._book_2.items()}

    @route(r'/getTopOrder1')
    def get_top_order_1(self, routes, params):
        return next(iter(self._book_1))

    @route(r'/getTopOrder2')
    def get_top_order_2(self, routes, params):
        return next(iter(self._book_2))

    @route(r'/getAllOrders1')
    def get_all_orders_1(self, routes, params):
        return self._book_1

    @route(r'/getAllOrders2')
    def get_all_orders_2(self, routes, params):
        return self._book_2

    @route(r'/trade1')
    def trade_1(self, routes, params):
        if not params:
            return {'error': 'Invalid request'}
        side = params.get('side')
        price = float(params.get('price', 0))
        size = int(params.get('size', 0))
        if side not in ops or price <= 0 or size <= 0:
            return {'error': 'Invalid request'}
        orders = []
        if side == 'buy':
            orders = [order for order in self._book_1 if ops[side](order, price)]
        elif side == 'sell':
            orders = [order for order in self._book_1 if ops[side](order, price)]
        for order in orders:
            if size > 0:
                trades = self._book_1.pop(order)
                trades = trades[:size]
                if len(trades) == size:
                    size = 0
                else:
                    size -= len(trades)
                yield from trades
        return {'error': 'Not enough shares available'}

    @route(r'/trade2')
    def trade_2(self, routes, params):
        if not params:
            return {'error': 'Invalid request'}
        side = params.get('side')
        price = float(params.get('price', 0))
        size = int(params.get('size', 0))
        if side not in ops or price <= 0 or size <= 0:
            return {'error': 'Invalid request'}
        orders = []
        if side == 'buy':
            orders = [order for order in self._book_2 if ops[side](order, price)]
        elif side == 'sell':
            orders = [order for order in self._book_2 if ops[side](order, price)]
        for order in orders:
            if size > 0:
                trades = self._book_2.pop(order)
                trades = trades[:size]
                if len(trades) == size:
                    size = 0
                else:
                    size -= len(trades)
                yield from trades
        return {'error': 'Not enough shares available'}


    @route('/query')
    def handle_query(self, x, order_book=None):
        """ Takes no arguments, and yields the current top of the book;  the
            best bid and ask and their sizes, followed by the best bid and
            ask that aren't at the top of the book, each of these with the
            other side of the book, followed by the last trade.  When the
            book is crossed, the trade is output before the best bid or ask,
            at the appropriate size.

            Note that there's no guarantee the volume of the trades is the
            same as the size of the order that generated them, especially
            when crossing a book.

            This function is the primary driver of the trading simulation.
        """
        while True:
            last_price = 0.0
            last_size = 0
            best_bid = 0.0
            best_ask = 0.0
            best_bid_size = 0
            best_ask_size = 0
            # Determine the best bid and ask, and their sizes.
            for bid, asks in self._book_1.items():
                if bid > best_bid:
                    best_bid = bid
                    best_bid_size = sum(x[1] for x in asks)
            for ask, bids in self._book_1.items():
                if ask < best_ask or not best_ask:
                    best_ask = ask
                    best_ask_size = sum(x[1] for x in bids)
            # If the bid and ask have crossed, generate a trade.
            if best_bid and best_bid >= best_ask:
                if last_price < best_ask:
                    yield (datetime.now(), 'ABC', 'trade', best_ask,
                           last_size + sum(x[1] for x in self._book_1.pop(best_ask)))
                    last_price = best_ask
            # Output the best bid and ask, and their sizes.
            yield (datetime.now(), 'ABC', 'bid', best_bid, best_bid_size)
            yield (datetime.now(), 'ABC', 'ask', best_ask, best_ask_size)
            # Output the next 5 bid and ask levels.
            bids = list(filter(lambda x: x[0] > best_bid, self._book_1.keys()))
            asks = list(filter(lambda x: x[0] < best_ask, self._book_1.keys()))
            for i in range(5):
                if bids:
                    yield (datetime.now(), 'ABC', 'bid', bids[0], sum(x[1] for x in self._book_1[bids[0]]))
                    bids = bids[1:]
                if asks:
                    yield (datetime.now(), 'ABC', 'ask', asks[0], sum(x[1] for x in self._book_1[asks[0]]))
                    asks = asks[1:]
            # Generate the next trade.
            if not bids:
                while True:
                    # Update the book with the next data.
                    try:
                        now, stock, side, price, size = next(self._data_1)
                        if now > self._sim_start + self._rt_since_sim:
                            self._rt_since_sim = now - self._sim_start
                    except StopIteration:
                        self._data_1 = order_book(read_csv(), self._book_1, 'ABC')
                        self._sim_start, _, _ = next(self._data_1)
                        self._rt_since_sim = timedelta(0)
                        continue
                    break
                if side == 'buy':
                    self._book_1.setdefault(price, []).append((now, size))
                else:
                    self._book_1.setdefault(price, []).append((now, -size))
            if not asks:
                while True:
                    # Update the book with the next data.
                    try:
                        now, stock, side, price, size = next(self._data_1)
                        if now > self._sim_start + self._rt_since_sim:
                            self._rt_since_sim = now - self._sim_start
                    except StopIteration:
                        self._data_1 = order_book(read_csv(), self._book_1, 'ABC')
                        self._sim_start, _, _ = next(self._data_1)
                        self._rt_since_sim = timedelta(0)
                        continue
                    break
                if side == 'buy':
                    self._book_1.setdefault(price, []).append((now, size))
                else:
                    self._book_1.setdefault(price, []).append((now, -size))

            # Determine the best bid and ask, and their sizes.
            for bid, asks in self._book_2.items():
                if bid > best_bid:
                    best_bid = bid
                    best_bid_size = sum(x[1] for x in asks)
            for ask, bids in self._book_2.items():
                if ask < best_ask or not best_ask:
                    best_ask = ask
                    best_ask_size = sum(x[1] for x in bids)
            # If the bid and ask have crossed, generate a trade.
            if best_bid and best_bid >= best_ask:
                if last_price < best_ask:
                    yield (datetime.now(), 'DEF', 'trade', best_ask,
                           last_size + sum(x[1] for x in self._book_2.pop(best_ask)))
                    last_price = best_ask
            # Output the best bid and ask, and their sizes.
            yield (datetime.now(), 'DEF', 'bid', best_bid, best_bid_size)
            yield (datetime.now(), 'DEF', 'ask', best_ask, best_ask_size)
            # Output the next 5 bid and ask levels.
            bids = list(filter(lambda x: x[0] > best_bid, self._book_2.keys()))
            asks = list(filter(lambda x: x[0] < best_ask, self._book_2.keys()))
            for i in range(5):
                if bids:
                    yield (datetime.now(), 'DEF', 'bid', bids[0], sum(x[1] for x in self._book_2[bids[0]]))
                    bids = bids[1:]
                if asks:
                    yield (datetime.now(), 'DEF', 'ask', asks[0], sum(x[1] for x in self._book_2[asks[0]]))
                    asks = asks[1:]
            # Generate the next trade.
            if not bids:
                while True:
                    # Update the book with the next data.
                    try:
                        now, stock, side, price, size = next(self._data_2)
                        if now > self._sim_start + self._rt_since_sim:
                            self._rt_since_sim = now - self._sim_start
                    except StopIteration:
                        self._data_2 = order_book(read_csv(), self._book_2, 'DEF')
                        self._sim_start, _, _ = next(self._data_2)
                        self._rt_since_sim = timedelta(0)
                        continue
                    break
                if side == 'buy':
                    self._book_2.setdefault(price, []).append((now, size))
                else:
                    self._book_2.setdefault(price, []).append((now, -size))
            if not asks:
                while True:
                    # Update the book with the next data.
                    try:
                        now, stock, side, price, size = next(self._data_2)
                        if now > self._sim_start + self._rt_since_sim:
                            self._rt_since_sim = now - self._sim_start
                    except StopIteration:
                        self._data_2 = order_book(read_csv(), self._book_2, 'DEF')
                        self._sim_start, _, _ = next(self._data_2)
                        self._rt_since_sim = timedelta(0)
                        continue
                    break
                if side == 'buy':
                    self._book_2.setdefault(price, []).append((now, size))
                else:
                    self._book_2.setdefault(price, []).append((now, -size))


def generate_csv():
    """ Generates a CSV file containing the trading data. """
    app = App()
    rows = []
    for data in app.handle_query():
        rows.append(data)
    with open('trading_data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Time', 'Stock', 'Type', 'Price', 'Size'])
        writer.writerows(rows)


if __name__ == '__main__':
    generate_csv()
