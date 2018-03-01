import time
import base64
import hmac
import urllib.request
import urllib.parse
import urllib.error
import hashlib
import sys
import json
import requests
import pysher
import logging

class Bitstamp():
    balance_url = "https://www.bitstamp.net/api/v2/balance/"
    buy_url = "https://www.bitstamp.net/api/v2/buy/btcusd/"
    sell_url = "https://www.bitstamp.net/api/v2/sell/btcusd/"

    def __init__(self, apiID = None, apiKey=None, apiSecret=None):
        self.proxydict = None
        self.client_id = apiID
        self.api_key = apiKey
        self.api_secret = apiSecret 
        self.logger = logging.getLogger('root')
        self.pusher = pysher.Pusher("de504dc5763aeef9ff52")
        self.pusher.connection.bind('pusher:connection_established', self.connect_handler)
        self.pusher.connect() 
        self.depth_data = None
        self.update_time = time.time()
        #wait for data
        self._wait_for_data()


    def _wait_for_data(self):
        for x in range(1,20):
            time.sleep(0.1)
            if self.depth_data != None:
                break

    def channel_callback(self, data):
        self.depth_data = json.loads(data)
        self.update_time = time.time()


    def connect_handler(self, data):
        self.logger.info("### bitstamp ws opened ###")
        channel_name = "order_book"

        self.channel = self.pusher.subscribe(channel_name)

        self.channel.bind('data', self.channel_callback)

    def get_ticker(self):
        data = self.depth_data
        if data != None:
            bid = float(data['bids'][0][0])
            ask = float(data['asks'][0][0])
            ticker = {
                "update_time": self.update_time,
                "buy": bid,
                "sell": ask,
                "mid": (bid + ask) / 2
            }
            return ticker
        
    def _create_nonce(self):
        return int(time.time() * 1000000)

    def _send_request(self, url, params={}, extra_headers=None):
        headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'User-Agent': 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
        }
        if extra_headers is not None:
            for k, v in extra_headers.items():
                headers[k] = v
        nonce = str(self._create_nonce())
        message = nonce + self.client_id + self.api_key
        if sys.version_info.major == 2:
            signature = hmac.new(self.api_secret, msg=message, digestmod=hashlib.sha256).hexdigest().upper()
        else:
            signature = hmac.new(str.encode(self.api_secret), msg=str.encode(message), digestmod=hashlib.sha256).hexdigest().upper()
        params['key'] = self.api_key
        params['signature'] = signature
        params['nonce'] = nonce
        #postdata = urllib.parse.urlencode(params).encode("utf-8")
        #req = urllib.request.Request(url, postdata, headers=headers)
        #print ("req=", postdata)
        #response = urllib.request.urlopen(req)
        response = requests.post(url, data=params, proxies=self.proxydict)
        #code = response.getcode()
        code = response.status_code
        if code == 200:
            #jsonstr = response.read().decode('utf-8')
            #return json.loads(jsonstr)
            return response.json()
        elif code == 403:
            raise Exception('please active your apikey')
            return 
        else:
            raise Exception('bitmex rest api error. code: %d' % (code))
        return None

    def _buy(self, amount, price):
        """Create a buy limit order"""
        params = {"amount": round(amount,8), "price": round(price,2)}
        response = self._send_request(self.buy_url, params)
        if "status" in response and "error" == response["status"]:
            raise Exception(response["reason"])

    def _sell(self, amount, price):
        """Create a sell limit order"""
        params = {"amount": round(amount,8), "price": round(price,2)}
        response = self._send_request(self.sell_url, params)
        if "status" in response and "error" == response["status"]:
            raise Exception(response["reason"])

    def get_info(self):
        """Get balance"""
        response = self._send_request(self.balance_url)
        if "status" in response and "error" == response["status"]:
            raise Exception(response["reason"])
            return

        #print(json.dumps(response)) 
        return response      
 

