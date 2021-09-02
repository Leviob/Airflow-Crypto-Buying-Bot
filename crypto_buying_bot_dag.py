import logging 
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from datetime import datetime, timedelta
import math
from pprint import pprint
from bot_api_functions import place_limit_order, get_active_orders, get_trade_history
from airflow.models import Variable


SYMBOL = 'ETHUSD'
GEMINI_CRYPTO_TRADING_MINIMUM = 0.001 # This is the minimum trade amount for this symbol that Gemini specifies here: 
    # https://support.gemini.com/hc/en-us/articles/4401824250267-Are-there-trading-minimums- 
MIN_PURCHASE_AMOUNT = 5 # The minimum amount the bot will purchase
MAX_PURCHASE_AMOUNT = 40 # The maximum amount the bot will purchase
GOOD_RATIO = 0.85 # Ratio of current price and average price that indicates a good price
POOR_RATIO = 1.2 # Ratio of current price and average price that indicates a poor price
DANGER_RATIO = 1.5 # Ratio of current price and average price that indicates a dangerously high price
LENGTH_OF_PRICE_AVERAGE = 100 # candle period is 6hr, so 100 periods is 25 days.
EMAIL = Variable.get('email_address')

def determine_value():
    '''
    Determines the value of a cryptocurrencies current price.

    This is accomplished by comparing the price with an average
    of previous periods.

    Returns:
        float: A number between 0 and 1 corresponding to the value
            where 0 is the lowest value and 1 is the highest.
            Value is determined using GOOD_RATIO, POOR_RATIO, and
            LENGTH_OF_PRICE_AVERAGE.

        float: The most recent asking price
    '''
    # TODO: Use asincio here to call multiple apis asyncronously
    response = requests.get(f'https://api.sandbox.gemini.com/v2/candles/{SYMBOL}/6hr')  # Returns 60 days worth of 6 hour candles
    crypto_candle_data = response.json()
    response = requests.get(f'https://api.sandbox.gemini.com/v1/pubticker/{SYMBOL}') # Returns current ticker info like bid, ask and last
    preliminary_ask_price = float(response.json()['ask'])
    logging.debug(f'current asking price: {preliminary_ask_price}')

    average_price = sum([x[4] for x in crypto_candle_data[LENGTH_OF_PRICE_AVERAGE-1::-1]])/LENGTH_OF_PRICE_AVERAGE # Index 4 is the close price
    price_rel_to_avg = preliminary_ask_price / average_price

    logging.info(f'''price rel to avg: {price_rel_to_avg}
    ask price: {preliminary_ask_price}
    average price: {average_price}''')

    logging.debug(f'avg:{average_price} price/avg: {price_rel_to_avg}')
    if price_rel_to_avg > DANGER_RATIO or (preliminary_ask_price - float(response.json()['bid']) > 10): # Asking price is abnormally high 
        raise Exception('Asking price is too high relative to the average or the current bid.')

    # Else, price is fair
    # Return how valuable current price is as a ratio between the poor and good ratios
    value_of_current_price = round(1 - ((price_rel_to_avg - GOOD_RATIO) / (POOR_RATIO - GOOD_RATIO)), 2)
    value_of_current_price = max((min(1, value_of_current_price)), 0) # Only allow values between 0 and 1
    logging.debug(f'value: {value_of_current_price}')

    return value_of_current_price, preliminary_ask_price

def place_order(**kwargs):
    '''
    Places a limit order for an amount corresponding to the price's value.
    '''
    ti = kwargs['ti']
    value_of_current_price, preliminary_ask_price = ti.xcom_pull(task_ids='determine_value_task')
    # Amount to purchase is min or max if the price is bad or good respectively
    if value_of_current_price == 0:
        purchase_amount_in_usd = MIN_PURCHASE_AMOUNT
    elif value_of_current_price == 1:
        purchase_amount_in_usd = MAX_PURCHASE_AMOUNT
    else:
        # When the value is close to fair, the amount to purchase is decided using a sigmoid function with 
        # value_of_current_price as input
        sigmoid =  1 / (1 + math.exp(-10*(value_of_current_price - 0.5)))
        purchase_amount_in_usd = round(MIN_PURCHASE_AMOUNT + ((MAX_PURCHASE_AMOUNT - MIN_PURCHASE_AMOUNT) * sigmoid), 2)

        logging.info(f'the current sigmoid value is {sigmoid}')
    logging.info(f'I\'m purchasing {purchase_amount_in_usd} USD worth because the value is {value_of_current_price}')

    # Get an updated current ask price
    response = requests.get(f'https://api.sandbox.gemini.com/v1/pubticker/{SYMBOL}')
    current_ask_price = min(float(response.json()['ask']), preliminary_ask_price) # If price has changed durring code execution, use lower price
    buy_order_price = current_ask_price - 1 # One dollar less than ask
    purchase_amount_in_crypto = round(purchase_amount_in_usd/buy_order_price,6)

    logging.info(f'current_ask_price: {current_ask_price}\n \
        buy_order_price: {buy_order_price}\n \
        purchase_amount_in_crypto: {purchase_amount_in_crypto}\n \
        purchase_amount_in_usd: {purchase_amount_in_usd}')

    if purchase_amount_in_crypto <= GEMINI_CRYPTO_TRADING_MINIMUM:
        raise Exception(f'Min purchase amount for {SYMBOL} is set as {GEMINI_CRYPTO_TRADING_MINIMUM}. Attempted order is for {purchase_amount_in_crypto}.')

    # This is where the order gets placed
    json_response = place_limit_order(SYMBOL, purchase_amount_in_crypto, buy_order_price)
    logging.debug(json_response)    
    # print(f'The current asking price is {current_ask_price}')
    # print(f'The current value of this coin is {value_of_current_price}')

    if 'is_cancelled' in json_response:
        if json_response['is_cancelled'] == False:
            logging.info(f'Order sucessfully placed for {purchase_amount_in_crypto} {SYMBOL} costing {purchase_amount_in_usd} USD.')
        else:
            logging.warning('Order was cancelled')
            raise Exception(json_response['reason'])

    else: # if 'is_cancelled' isn't in response, probably an error.
        try:
            if json_response['result'] == 'error':
                raise Exception(json_response['message'])
        except:
            logging.warning(json_response)
            raise Exception('Something went wrong.')

def find_filled_orders():
    '''
    Generates a list of all orders placed by this bot which have been filled. 

    Returns: a list of dictionaries in json format corresponding to each order.
    '''
    '''
    Get entire trade history each time this is run. This can be made more efficient by only looking at 
      some recent trades and appending the values in a database.
    '''
    trade_history = get_trade_history()
    bot_trade_history = []
    for order in trade_history:
        if 'client_order_id' in order and order['client_order_id'][:6] == 'bot_v1':
            bot_trade_history.append(order)
    return bot_trade_history

def count_open_orders():
    open_orders = get_active_orders()
    open_order_ids = set()
    for order in open_orders:
        open_order_ids.add(order['order_id'])
    return len(open_order_ids)

def analyze_trades(**kwargs):
    '''
    Calculates basic metrics from completed orders.

    Metrics like total USD spent, total crypto purchased, and percent return
        made using this strategy are calculated and printed.
    '''
    ti = kwargs['ti']
    bot_trade_history = ti.xcom_pull(task_ids='find_filled_orders_task')
    value, ask_price = ti.xcom_pull(task_ids='determine_value_task')
    open_order_count = ti.xcom_pull(task_ids='count_open_orders_task')
    bot_order_count = 0
    total_usd_spent = 0
    total_crypto_qty_purchased = 0
    list_of_purchase_prices = []
    total_fees_reported = 0
    filled_order_ids = set()

    for order in bot_trade_history:
        filled_order_ids.add(order['order_id'])
        total_usd_spent += (float(order['price']) * float(order['amount']))
        total_crypto_qty_purchased += float(order['amount'])
        list_of_purchase_prices.append(float(order['price']))
        total_fees_reported += float(order['fee_amount'])
    bot_order_count = len(bot_trade_history)

    '''
    Calculate Theoretical return using DCA strategy to compare results.
    This calculation for average spent is probably bogus because the list of purchase prices 
    is less than a dollar for some trades. It will likely be much more accurate outside the sandbox.'''
    if list_of_purchase_prices:
        average_spent = total_usd_spent/len(list_of_purchase_prices)
    else:
        average_spent = 0
    dca_crypto_purchased = 0
    for price in list_of_purchase_prices:
        dca_crypto_purchased += average_spent / price
    dca_percent_return = (dca_crypto_purchased * ask_price) / total_usd_spent
    # TODO: tally amount spent and see if any hidden fees are being removed from transactions
    # total_fees_calculated = 

    percent_return = (total_crypto_qty_purchased * ask_price) / total_usd_spent

    logging.info(f'My percent return on my investments using this bot is: {percent_return}')
    logging.info(f'My percent return had I used a Dollar Cost Averaging strategy is approximately: {dca_percent_return}')
    logging.info(f'{len(filled_order_ids)} bot trades have filled is this version over a total of {bot_order_count} individual orders.')
    logging.info(f'{open_order_count} order(s) still pending')
    logging.info(f'{total_crypto_qty_purchased} in {SYMBOL} purchased.')
    logging.info(f'{total_usd_spent} in usd spent (approximated).')
    logging.info(f'{total_fees_reported} paid in fees.')   
    # pprint(bot_trade_history) # For debugging, print all trades

default_args = {
    'owner': 'leviob',
    'depends_on_past': False,
    'retries': 50,
    'retry_delay': timedelta(seconds=10),
    'email': EMAIL
}

dag = DAG(
    'crypto_dva_bot',
    'Buy crypto regularly using Dollar Value Averaging strategy',
    # Start now
    start_date=datetime.now() - timedelta(days=3),
    # Run every 3 days
    schedule_interval=timedelta(days=3),
    default_args=default_args,
    catchup=False # necessary for starting at datetime.now(), otherwise starts at end of last run
)

determine_value_task = PythonOperator(
    task_id='determine_value_task',
    dag=dag,
    python_callable=determine_value
)

place_order_task = PythonOperator(
    task_id='place_order_task',
    dag=dag,
    python_callable=place_order
)

find_filled_orders_task = PythonOperator(
    task_id='find_filled_orders_task',
    dag=dag,
    python_callable=find_filled_orders
)

count_open_orders_task = PythonOperator(
    task_id='count_open_orders_task',
    dag=dag,
    python_callable=count_open_orders
)

analyze_trades_task = PythonOperator(
    task_id='analyze_trades_task',
    dag=dag,
    python_callable=analyze_trades
)

determine_value_task >> place_order_task >> [find_filled_orders_task, count_open_orders_task] >> analyze_trades_task
