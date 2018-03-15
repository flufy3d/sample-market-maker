from __future__ import absolute_import
from time import sleep,time
import sys
from datetime import datetime
from os.path import getmtime
import random
import requests
import atexit
import signal
import traceback

from market_maker import bitmex
from market_maker import bitstamp
from market_maker.telegram import send_message
from market_maker.settings import settings
from market_maker.utils import log, constants, errors, math

# Used for reloading the bot - saves modified times of key files
import os
watched_files_mtimes = [(f, getmtime(f)) for f in settings.WATCHED_FILES]


#
# Helpers
#
logger = log.setup_custom_logger('root')


class ExchangeInterface:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        if len(sys.argv) > 1:
            self.symbol = sys.argv[1]
        else:
            self.symbol = settings.SYMBOL
        self.bitmex = bitmex.BitMEX(base_url=settings.BASE_URL, symbol=self.symbol,
                                    apiKey=settings.API_KEY, apiSecret=settings.API_SECRET,
                                    orderIDPrefix=settings.ORDERID_PREFIX, postOnly=settings.POST_ONLY,
                                    timeout=settings.TIMEOUT)

    def cancel_order(self, order):
        tickLog = self.get_instrument()['tickLog']
        logger.info("Canceling: %s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))
        while True:
            try:
                self.bitmex.cancel(order['orderID'])
                sleep(settings.API_REST_INTERVAL)
            except ValueError as e:
                logger.info(e)
                sleep(settings.API_ERROR_INTERVAL)
            else:
                break

    def cancel_all_orders(self):
        if self.dry_run:
            return

        logger.info("Resetting current position. Canceling all existing orders.")
        tickLog = self.get_instrument()['tickLog']

        # In certain cases, a WS update might not make it through before we call this.
        # For that reason, we grab via HTTP to ensure we grab them all.
        orders = self.bitmex.http_open_orders()

        for order in orders:
            logger.info("Canceling: %s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))

        if len(orders):
            self.bitmex.cancel([order['orderID'] for order in orders])

        sleep(settings.API_REST_INTERVAL)

    def get_portfolio(self):
        contracts = settings.CONTRACTS
        portfolio = {}
        for symbol in contracts:
            position = self.bitmex.position(symbol=symbol)
            instrument = self.bitmex.instrument(symbol=symbol)

            if instrument['isQuanto']:
                future_type = "Quanto"
            elif instrument['isInverse']:
                future_type = "Inverse"
            elif not instrument['isQuanto'] and not instrument['isInverse']:
                future_type = "Linear"
            else:
                raise NotImplementedError("Unknown future type; not quanto or inverse: %s" % instrument['symbol'])

            if instrument['underlyingToSettleMultiplier'] is None:
                multiplier = float(instrument['multiplier']) / float(instrument['quoteToSettleMultiplier'])
            else:
                multiplier = float(instrument['multiplier']) / float(instrument['underlyingToSettleMultiplier'])

            portfolio[symbol] = {
                "currentQty": float(position['currentQty']),
                "futureType": future_type,
                "multiplier": multiplier,
                "markPrice": float(instrument['markPrice']),
                "spot": float(instrument['indicativeSettlePrice'])
            }

        return portfolio

    def calc_delta(self):
        """Calculate currency delta for portfolio"""
        portfolio = self.get_portfolio()
        spot_delta = 0
        mark_delta = 0
        for symbol in portfolio:
            item = portfolio[symbol]
            if item['futureType'] == "Quanto":
                spot_delta += item['currentQty'] * item['multiplier'] * item['spot']
                mark_delta += item['currentQty'] * item['multiplier'] * item['markPrice']
            elif item['futureType'] == "Inverse":
                spot_delta += (item['multiplier'] / item['spot']) * item['currentQty']
                mark_delta += (item['multiplier'] / item['markPrice']) * item['currentQty']
            elif item['futureType'] == "Linear":
                spot_delta += item['multiplier'] * item['currentQty']
                mark_delta += item['multiplier'] * item['currentQty']
        basis_delta = mark_delta - spot_delta
        delta = {
            "spot": spot_delta,
            "mark_price": mark_delta,
            "basis": basis_delta
        }
        return delta

    def get_delta(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.get_position(symbol)['currentQty']

    def get_instrument(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.instrument(symbol)

    def get_margin(self):
        if self.dry_run:
            return {'marginBalance': float(settings.DRY_BTC), 'availableFunds': float(settings.DRY_BTC)}
        return self.bitmex.funds()

    def get_orders(self):
        if self.dry_run:
            return []
        return self.bitmex.open_orders()

    def get_highest_buy(self):
        buys = [o for o in self.get_orders() if o['side'] == 'Buy']
        if not len(buys):
            return {'price': -2**32}
        highest_buy = max(buys or [], key=lambda o: o['price'])
        return highest_buy if highest_buy else {'price': -2**32}

    def get_lowest_sell(self):
        sells = [o for o in self.get_orders() if o['side'] == 'Sell']
        if not len(sells):
            return {'price': 2**32}
        lowest_sell = min(sells or [], key=lambda o: o['price'])
        return lowest_sell if lowest_sell else {'price': 2**32}  # ought to be enough for anyone

    def get_position(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.position(symbol)

    def get_ticker(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.ticker_data(symbol)

    def is_open(self):
        """Check that websockets are still open."""
        return not self.bitmex.ws.exited

    def check_market_open(self):
        instrument = self.get_instrument()
        if instrument["state"] != "Open" and instrument["state"] != "Closed":
            raise errors.MarketClosedError("The instrument %s is not open. State: %s" %
                                           (self.symbol, instrument["state"]))

    def check_if_orderbook_empty(self):
        """This function checks whether the order book is empty"""
        instrument = self.get_instrument()
        if instrument['midPrice'] is None:
            raise errors.MarketEmptyError("Orderbook is empty, cannot quote")

    def place_order_raw(self, order, isclose):
        if self.dry_run:
            return order
        return self.bitmex.place_order_raw(order, isclose)

    def amend_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.amend_bulk_orders(orders)

    def create_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.create_bulk_orders(orders)

    def cancel_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.cancel([order['orderID'] for order in orders])


class OrderManager:
    def __init__(self):
        self.exchange = ExchangeInterface(settings.DRY_RUN)
        # Once exchange is created, register exit handler that will always cancel orders
        # on any error.
        self.exchange1 = bitstamp.Bitstamp(settings.bitstamp_client_id,settings.bitstamp_api_key,settings.bitstamp_api_secret)

        atexit.register(self.exit)
        signal.signal(signal.SIGTERM, self.exit)

        logger.info("Using symbol %s." % self.exchange.symbol)

        if settings.DRY_RUN:
            logger.info("Initializing dry run. Orders printed below represent what would be posted to BitMEX.")
        else:
            logger.info("Order Manager initializing, connecting to BitMEX. Live run: executing real trades.")

        self.start_time = datetime.now()
        self.instrument = self.exchange.get_instrument()
        self.starting_qty = self.exchange.get_delta()
        self.running_qty = self.starting_qty
        self.last_exec_time = time()
        self.log_limit_exceeded = True
        self.first_run = True
        self.reset()

    def reset(self):
        self.exchange.cancel_all_orders()
        self.sanity_check()



    def get_ticker(self):
        ticker = self.exchange.get_ticker()
        return ticker

    def get_ticker1(self):
        ticker = self.exchange1.get_ticker()
        return ticker

    def get_fund_rate(self):
        data = self.exchange.get_instrument()
        timestamp_str = data['timestamp']
        timestamp_str = timestamp_str[:-5]
        datetime_object = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')


        h = datetime_object.hour
        m = datetime_object.minute
        offset = (h+4)%8
        timepast = offset*60+m
        weight = timepast/(60*8)

        ret = {
            'rate':data['fundingRate'],
            'irate':data['indicativeFundingRate'],
            'weight':weight

        }
        return ret

    def get_price_offset(self, index):
        """Given an index (1, -1, 2, -2, etc.) return the price for that side of the book.
           Negative is a buy, positive is a sell."""
        # Maintain existing spreads for max profit
        if settings.MAINTAIN_SPREADS:
            start_position = self.start_position_buy if index < 0 else self.start_position_sell
            # First positions (index 1, -1) should start right at start_position, others should branch from there
            index = index + 1 if index < 0 else index - 1
        else:
            # Offset mode: ticker comes from a reference exchange and we define an offset.
            start_position = self.start_position_buy if index < 0 else self.start_position_sell

            # If we're attempting to sell, but our sell price is actually lower than the buy,
            # move over to the sell side.
            if index > 0 and start_position < self.start_position_buy:
                start_position = self.start_position_sell
            # Same for buys.
            if index < 0 and start_position > self.start_position_sell:
                start_position = self.start_position_buy

        return math.toNearest(start_position * (1 + settings.INTERVAL) ** index, self.instrument['tickSize'])

    ###
    # Orders
    ###

    def place_orders(self):
        """Create order items for use in convergence."""

        buy_orders = []
        sell_orders = []
        # Create orders from the outside in. This is intentional - let's say the inner order gets taken;
        # then we match orders from the outside in, ensuring the fewest number of orders are amended and only
        # a new order is created in the inside. If we did it inside-out, all orders would be amended
        # down and a new order would be created at the outside.
        for i in reversed(range(1, settings.ORDER_PAIRS + 1)):
            if not self.long_position_limit_exceeded():
                buy_orders.append(self.prepare_order(-i))
            if not self.short_position_limit_exceeded():
                sell_orders.append(self.prepare_order(i))


        return self.converge_orders(buy_orders, sell_orders)

    def prepare_order(self, index):
        """Create an order object."""

        if settings.RANDOM_ORDER_SIZE is True:
            quantity = random.randint(settings.MIN_ORDER_SIZE, settings.MAX_ORDER_SIZE)
        else:
            quantity = settings.ORDER_START_SIZE + ((abs(index) - 1) * settings.ORDER_STEP_SIZE)

        price = self.get_price_offset(index)

        return {'price': price, 'orderQty': quantity, 'side': "Buy" if index < 0 else "Sell"}

    def converge_orders(self, buy_orders, sell_orders):
        """Converge the orders we currently have in the book with what we want to be in the book.
           This involves amending any open orders and creating new ones if any have filled completely.
           We start from the closest orders outward."""

        tickLog = self.exchange.get_instrument()['tickLog']
        to_amend = []
        to_create = []
        to_cancel = []
        buys_matched = 0
        sells_matched = 0
        existing_orders = self.exchange.get_orders()

        # Check all existing orders and match them up with what we want to place.
        # If there's an open one, we might be able to amend it to fit what we want.
        for order in existing_orders:
            try:
                if order['side'] == 'Buy':
                    desired_order = buy_orders[buys_matched]
                    buys_matched += 1
                else:
                    desired_order = sell_orders[sells_matched]
                    sells_matched += 1

                # Found an existing order. Do we need to amend it?
                if desired_order['orderQty'] != order['leavesQty'] or (
                        # If price has changed, and the change is more than our RELIST_INTERVAL, amend.
                        desired_order['price'] != order['price'] and
                        abs((desired_order['price'] / order['price']) - 1) > settings.RELIST_INTERVAL):
                    to_amend.append({'orderID': order['orderID'], 'orderQty': order['cumQty'] + desired_order['orderQty'],
                                     'price': desired_order['price'], 'side': order['side']})
            except IndexError:
                # Will throw if there isn't a desired order to match. In that case, cancel it.
                to_cancel.append(order)

        while buys_matched < len(buy_orders):
            to_create.append(buy_orders[buys_matched])
            buys_matched += 1

        while sells_matched < len(sell_orders):
            to_create.append(sell_orders[sells_matched])
            sells_matched += 1


        if len(to_create) > 0:
            logger.info("Creating %d orders:" % (len(to_create)))
            for order in reversed(to_create):
                logger.info("%4s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))
            
            try:
                if len(to_create) == 1:
                    _order = to_create[0]
                    pos = self.exchange.get_delta()
                    choose_side = ''
                    if pos > 0:
                        choose_side = 'Sell'
                    else:
                        choose_side = 'Buy'

                    if _order['side'] == choose_side:
                        logger.info("Only one order.try a close boost.")
                        if abs(pos) > 1000:
                            self.exchange.place_order_raw(_order,True)
                        else:
                            self.exchange.place_order_raw(_order,False)
                    else:
                        logger.info("Only one order.just place order.")
                        self.exchange.place_order_raw(_order,False)

                else:
                    self.exchange.create_bulk_orders(to_create)
            except Exception as e:
                logger.warn("Creating failed. Try Again.")
                self.last_exec_time = time() - settings.LOOP_INTERVAL*0.8

                
        if len(to_amend) > 0:
            for amended_order in reversed(to_amend):
                reference_order = [o for o in existing_orders if o['orderID'] == amended_order['orderID']][0]
                sys.stdout.write("Amending %4s: %d @ %.*f to %d @ %.*f (%+.*f)\n" % (
                    amended_order['side'],
                    reference_order['leavesQty'], tickLog, reference_order['price'],
                    (amended_order['orderQty'] - reference_order['cumQty']), tickLog, amended_order['price'],
                    tickLog, (amended_order['price'] - reference_order['price'])
                ))
            # This can fail if an order has closed in the time we were processing.
            # The API will send us `invalid ordStatus`, which means that the order's status (Filled/Canceled)
            # made it not amendable.
            # If that happens, we need to catch it and re-tick.
            try:
                self.exchange.amend_bulk_orders(to_amend)
            except Exception as e:
                logger.warn("Amending failed. Try Again.")
                self.last_exec_time = time() - settings.LOOP_INTERVAL*0.8


        # Could happen if we exceed a delta limit
        if len(to_cancel) > 0:
            logger.info("Canceling %d orders:" % (len(to_cancel)))
            for order in reversed(to_cancel):
                logger.info("%4s %d @ %.*f" % (order['side'], order['leavesQty'], tickLog, order['price']))
            
            try:
                self.exchange.cancel_bulk_orders(to_cancel)
            except Exception as e:
                logger.warn("Canceling failed. Try Again.")
                self.last_exec_time = time() - settings.LOOP_INTERVAL*0.8

    ###
    # Position Limits
    ###

    def short_position_limit_exceeded(self):
        """Returns True if the short position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.exchange.get_delta()
        return position <= settings.MIN_POSITION

    def long_position_limit_exceeded(self):
        """Returns True if the long position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.exchange.get_delta()
        return position >= settings.MAX_POSITION

    ###
    # Sanity
    ##

    def sanity_check(self):
        """Perform checks before placing orders."""

        # Check if OB is empty - if so, can't quote.
        self.exchange.check_if_orderbook_empty()

        # Ensure market is still open.
        self.exchange.check_market_open()


        # Messaging if the position limits are reached
        long_e = self.long_position_limit_exceeded()
        short_e = self.short_position_limit_exceeded()
        if long_e and self.log_limit_exceeded:
            logger.info("Long delta limit exceeded")
            logger.info("Current Position: %.f, Maximum Position: %.f" %
                        (self.exchange.get_delta(), settings.MAX_POSITION))
            self.log_limit_exceeded = False


        if short_e and self.log_limit_exceeded:
            logger.info("Short delta limit exceeded")
            logger.info("Current Position: %.f, Minimum Position: %.f" %
                        (self.exchange.get_delta(), settings.MIN_POSITION))
            self.log_limit_exceeded = False

        if not (long_e or short_e):
            self.log_limit_exceeded = True


    ###
    # Running
    ###

    def check_file_change(self):
        """Restart if any files we're watching have changed."""
        for f, mtime in watched_files_mtimes:
            if getmtime(f) > mtime:
                self.restart()

    def check_connection(self):
        """Ensure the WS connections are still open."""
        return self.exchange.is_open()

    def exit(self):
        logger.info("Shutting down. All open orders will be cancelled.")
        try:
            self.exchange.cancel_all_orders()
            sleep(0.2)
            self.exchange.bitmex.exit()
        except errors.AuthenticationError as e:
            logger.info("Was not authenticated; could not cancel orders.")
        except Exception as e:
            logger.info("Unable to cancel orders: %s" % e)

        sys.exit()

    def find_start_postion(self):
        bitmex_ticker = self.get_ticker()
        bitstamp_ticker = self.get_ticker1()

        fundrate_tuple = self.get_fund_rate()
        fundrate = fundrate_tuple['rate'] + fundrate_tuple['irate']*fundrate_tuple['weight']

        greed_index = abs(fundrate_tuple['irate'])/0.00375*settings.TARGET_PROFIT_RATE
        greed_index = greed_index * 1.2

        br = greed_index + settings.TARGET_PROFIT_RATE + fundrate
        sr = greed_index + settings.TARGET_PROFIT_RATE - fundrate

        self.start_position_buy = bitstamp_ticker['buy'] * (1 - br)
        self.start_position_sell =  bitstamp_ticker['sell'] * (1 + sr)

        if self.start_position_buy > bitmex_ticker['buy']:
            self.start_position_buy = bitmex_ticker['buy']
        if self.start_position_sell < bitmex_ticker['sell']:
            self.start_position_sell = bitmex_ticker['sell']



    def process_position_change(self):
        bitstamp_ticker = self.get_ticker1()

        _d = self.exchange.get_delta() - self.running_qty

        #In order to immediately deal
        _discount = 0.1

        _d_abs = abs(_d)
        if _d_abs > 5000:
            raise Exception('abnormal situation,too many delta change!')

        if  _d_abs > 10:
            logger.info('detect position change,current %d ,delta %d' % (self.running_qty,_d))
            send_message('detect position change,current %d ,delta %d' % (self.running_qty,_d))
            if _d > 0:
                price = float(bitstamp_ticker['buy'])
                amount = _d_abs / price
                logger.info("sell %d usd %.4f btc @%.2f bitstamp" % (_d_abs,amount,price))
                send_message("sell %d usd %.4f btc @%.2f bitstamp" % (_d_abs,amount,price))
                self.exchange1._sell(amount,price*(1.0 - _discount))
            else:
                price = float(bitstamp_ticker['sell'])
                amount = _d_abs / price
                logger.info("buy %d usd %.4f btc @%.2f bitstamp" % (_d_abs,amount,price))
                send_message("buy %d usd %.4f btc @%.2f bitstamp" % (_d_abs,amount,price))
                self.exchange1._buy(amount,price*(1.0 + _discount))


            #self.running_qty = self.exchange.get_delta()
            self.running_qty += _d
        elif _d_abs >= 1:
            logger.info("less than 10 usd we don't exec bitstamp order.")


    def do_once(self):
        self.find_start_postion()

        self.sanity_check()  

        self.place_orders()

        self.process_position_change()

    def run_loop(self):
        self.last_exec_time = time()
        while True:
            curTime = time()
            if curTime - self.last_exec_time > settings.LOOP_INTERVAL:

                sys.stdout.write("------------------\n")
                sys.stdout.flush()

                if not self.check_connection():
                    logger.error("Realtime data connection unexpectedly closed, restarting.")
                    self.restart()

                self.do_once()

                self.last_exec_time = curTime

                if self.first_run:
                    self.last_exec_time += 3
                    self.first_run = False
            else:
                #need high speed
                self.process_position_change()


            sleep(0.02)


    def restart(self):
        self.exchange.cancel_all_orders()
        logger.info("Restarting the market maker...")
        send_message("Restarting the market maker...")
        sleep(10)
        os.execv(sys.executable, [sys.executable] + sys.argv)

#
# Helpers
#


def XBt_to_XBT(XBt):
    return float(XBt) / constants.XBt_TO_XBT


def cost(instrument, quantity, price):
    mult = instrument["multiplier"]
    P = mult * price if mult >= 0 else mult / price
    return abs(quantity * P)


def margin(instrument, quantity, price):
    return cost(instrument, quantity, price) * instrument["initMargin"]


def run():

    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        logger.info('BBA Version: %s\n' % constants.VERSION)
        send_message('BBA Version: %s\n' % constants.VERSION)
        om = OrderManager()
        om.run_loop()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
    except Exception as e:
        s=traceback.format_exc()
        logger.error('crashed! error: %s' %(str(e)))
        send_message('crashed! error: %s' %(str(e)))
        logger.error(s)
        sys.exit()


