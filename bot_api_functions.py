import requests
import json
import base64
import hmac
import hashlib
import time
from datetime import datetime
from airflow.models import Variable
gemini_api_secret = Variable.get('gemini_api_secret').encode()
gemini_api_key = Variable.get('gemini_api_key')

def private_api_call(endpoint, payload):
    '''
    Calls the Gemini private api using the passed endpoint and payload variables.
    '''
    base_url = 'https://api.gemini.com'
    url = base_url + endpoint
    payload['nonce'] = str(int(time.time()*1000))
    encoded_payload = json.dumps(payload).encode()
    b64 = base64.b64encode(encoded_payload)
    signature = hmac.new(gemini_api_secret, b64, hashlib.sha384).hexdigest()

    request_headers = { 'Content-Type': 'text/plain',
                        'Content-Length': '0',
                        'X-GEMINI-APIKEY': gemini_api_key,
                        'X-GEMINI-PAYLOAD': b64,
                        'X-GEMINI-SIGNATURE': signature,
                        'Cache-Control': 'no-cache' }

    response = requests.post(url,
                            data=None,
                            headers=request_headers)
    response.raise_for_status()
    return response.json()

def place_limit_order(symbol, purchase_amount_in_crypto, buy_order_price):
    '''
    Places a new buy limit order.

    Requires the symbol, quantity of crypto, and desired purchase price
    to be included as arguments.

    Returns: A dictionary of the response in json format.
    '''
    endpoint = '/v1/order/new'
    client_order_id = f'bot_v2_{datetime.now().strftime("%Y-%m-%d_%H:%M")}'
    payload = {
    'request': '/v1/order/new',
        'symbol': symbol,
        'client_order_id': client_order_id,
        'amount': str(purchase_amount_in_crypto),
        'price': buy_order_price,
        'side': 'buy',
        'type': 'exchange limit',
        'options': ['maker-or-cancel']
    }

    return private_api_call(endpoint, payload)

def get_active_orders():
    '''
    Generates a list of dictionaries corresponding to each open order.
    
    Dictionaries include parameters of orders in json format.
    ''' 
    endpoint = '/v1/orders'
    payload = {
        'request': '/v1/orders'
    }    

    return private_api_call(endpoint, payload)
    
def get_trade_history(number_of_trades=None):
    '''
    Generates a list of dictionaries corresponding to each completed trade.

    Some trades are broken into many smaller trades. These are each returned
        as their own dictionary. Dictionaries include parameters of orders 
        in json format. Results are sorted from most recent orders first. 

    Args:
        number_of_trades(int): Optional. Limits the number of results to 
        this many of the most recent orders.
    '''
    endpoint = '/v1/mytrades'
    payload = {
        'request': '/v1/mytrades',
        'limit_trades': 500,
        'timestamp': 0
    }
    if number_of_trades == None: # Then return all trades
        all_trades = private_api_call(endpoint, payload)
        while True:
            payload['timestamp'] = all_trades[0]['timestamp'] + 1
            new_trades = private_api_call(endpoint, payload)
            if new_trades: # If returned list is not empty
                all_trades = new_trades + all_trades # Ordered such as to maintain descending timestamp order
            else:
                return all_trades

    else: # Else return the number_of_trades
        payload = {
            'request': '/v1/mytrades',
            'limit_trades': number_of_trades
        }    
        return private_api_call(endpoint, payload)
