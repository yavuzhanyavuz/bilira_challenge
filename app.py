# import main Flask class and request object
from flask import Flask, request, jsonify
import json
import bilira 

# create the Flask app
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

@app.route('/query-example')
def query_example():
    return 'Query String Example'

@app.route('/form-example')
def form_example():
    return 'Form Data Example'

@app.route('/market_order', methods=['POST'])
def market_order():
    request_data = request.get_json()

    action = request_data.get('Action')
    base_currency = request_data.get('Base_currency')
    quote_currency = request_data.get('Quote_currency')
    amount = request_data.get('Amount')

    with open('input_data.txt', 'w') as convert_file:
        convert_file.write(json.dumps(request_data))
    
    order_return = bilira.market_order(action=action, base_currency=base_currency, quote_currency=quote_currency, amount=amount)

    return jsonify(order_return)


@app.route('/limit_order', methods=['POST'])
def limit_order():
    request_data = request.get_json()

    action = request_data.get('Action')
    base_currency = request_data.get('Base_currency')
    quote_currency = request_data.get('Quote_currency')
    amount = request_data.get('Amount')
    price = request_data.get('Price')
    icebergs = request_data.get('Number_of_iceberg_order')

    with open('input_data.txt', 'w') as convert_file:
        convert_file.write(json.dumps(request_data))

    order_return = bilira.limit_order(action = action
                                    , base_currency = base_currency
                                    , quote_currency = quote_currency
                                    , amount = amount
                                    , price = price
                                    , iceberg= icebergs)

    return jsonify(order_return)

if __name__ == '__main__':
    # run app in debug mode on port 5000
    app.run(debug=True, port=5000)