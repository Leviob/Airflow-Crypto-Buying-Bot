# Airflow-Crypto-Buying-Bot
A cryptocurrency purchasing bot using a crude Dollar Value Averaging strategy.

## Introduction
The motivation for this program is to (1) automate the purchasing of cryptocurrency using Airflow, and (2) use a Dollar Value Averaging strategy to potentially improve the return. The return will be compared to the theoretical return using a DCA strategy with the same investment amount and timing. 

The following two investment strategies are useful in understanding this motivation. 

#### DCA
Dollar *Cost* Averaging (DCA) is basic investment strategy used to reduce the affect of market volatility on investments. By breaking an investment into many equal size investments over time, it minimizes risk of losing value if the market declines after investing. Multiple small investments averages out the volatility, producing a more consistent investment. 

#### DVA
Dollar *Value* Averaging (DVA) is another strategy. Similarly, a large investment is split into smaller amounts, but DVA differs in that the amount invested is modulated based on how good the current price seems. In theory, this can greater reduce the chances of losses from purchasing on multiple days when the price is coincidentally high. There exist more complex interpretations of this strategy, but this sufficiently describes how it will be implemented.

## Trading Strategy
The bot is composed of several tasks which make up a DAG, pictured below. 

[PICTURE]

The first task, `determine_value_task`, looks at the current and past prices to ascribe a "value" to the current price. If the price is high, it will have a low value, if the price is low, it will have a high value. The value is calculated by comparing the current price to the average price over the past several days. The next task, `place_order_task`, places an order for an amount of cryptocurrency based on the determined value. `find_filled_orders_task` will collect all the orders placed by the bot and `analyze_trades` will use them to calculate metrics on the orders. These metrics include the number of orders placed, the return, and the theoretical return if the same amount had been invested using the simpler DCA strategy. 

## How to Run
To run this automated purchasing bot, the following is required:
- A Gemini exchange account setup for use with the API
    - Details on setting up the API is on their website: https://docs.gemini.com/rest-api/#introduction 
- A server running Apache Airflow 
    - Airflow must be configured with the Gemini API key variables 
    - The API keys can be imported to Airflow using `variables.json`)
- *Optional:* in order to use email alerts for run failures, an SMTP host must be initialized 
    and configured in the `airflow.cfg` file 
    - The chosen email address can also be imported to Airflow using `variables.json`

The constant variables in `crypto_buying_bot_dag.py` can be adjusted to the desired specifications. 