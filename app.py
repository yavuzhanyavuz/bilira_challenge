# import main Flask class and request object
from flask import Flask, request, jsonify
import bilira 

# create the Flask app
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

@app.route('/')
def welcome_message():
    return 'Welcome aboard.'

@app.route('/market_order', methods=['POST'])
def market_order():
    request_data = request.get_json()
    request_data = {k.lower():v for k,v in request_data.items()}

    action = request_data.get('action')
    base_currency = request_data.get('base_currency')
    quote_currency = request_data.get('quote_currency')
    amount = request_data.get('amount')

    order_return = bilira.market_order(action=str(action).lower()
                                        , base_currency = base_currency
                                        , quote_currency = quote_currency
                                        , amount = amount)

    return jsonify(order_return)


@app.route('/limit_order', methods=['POST'])
def limit_order():
    request_data = request.get_json()
    request_data = {k.lower():v for k,v in request_data.items()}

    action = request_data.get('action')
    base_currency = request_data.get('base_currency')
    quote_currency = request_data.get('quote_currency')
    amount = request_data.get('amount')
    price = request_data.get('price')
    icebergs = request_data.get('number_of_iceberg_order')

    order_return = bilira.limit_order(action = str(action).lower()
                                    , base_currency = base_currency
                                    , quote_currency = quote_currency
                                    , amount = amount
                                    , price = price
                                    , iceberg= icebergs)

    return jsonify(order_return)

if __name__ == '__main__':
    # run app in debug mode on port 5000
    app.run(debug=True, port=5000)