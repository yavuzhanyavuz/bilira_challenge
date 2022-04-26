import pandas as pd, requests
from numbers import Number

from sqlalchemy import column
from userdefined_errors import MarketError, SymbolError, OrderbookError


def active_symbols():
    """
    Returns a dataframe of all symbols available on FTX SPOT market
    name: name as returned from FTX API
    ticker: former currency of the pair
    currency: latter currency of the pair
    Reverse_Quote: 1 if pair is reversed which is done manually in the function. 
    """

    ### sembolleri belirleyelim, bunları yanlıs input ayıklamakta kullanacagız
    response = requests.get('https://ftx.com/api/markets')
    data = response.json()

    try:
        names = data['result'] 
    except: 
        raise MarketError 
        
    ### Part 2: AFX'te spot için aktif olan currencyleri belirleyelim:

    symbols = []
    for row in names:
        if row['type'] == 'spot':
            symbols.append(row['name'])

    ### Part 2.2: Currencyleri ayıklayalım ve dataframede toplayalım. 
    # Inverse olmayan pairleri yani markette işlem gördüğü formda olanları 0 olarak  belirtiyoruz, inverse quotelar için inverseünü alıp 1 yazacağız.
    #  
    symbols_df = pd.DataFrame(symbols, columns=['name'], dtype='string')

    splitted_text = symbols_df['name'].str.split('/', n = 1, expand= True)
    symbols_df['ticker'] = splitted_text[0]
    symbols_df['currency'] = splitted_text[1]
    symbols_df['Reverse_Quote'] = 0

    ## inverse quoteları yazma kısmı
    reversed_df = symbols_df.copy()
    reversed_df['Reverse_Quote'] = 1
    reversed_df['currency'], reversed_df['ticker'] = reversed_df['ticker'], reversed_df['currency']
    reversed_df['name'] = reversed_df.ticker + '/' + reversed_df.currency

    symbols_df = pd.concat([symbols_df, reversed_df], axis = 0, ignore_index=True)

    return symbols_df


def get_orderbook(symbols_df, base_symbol, quote_symbol, action):
    """
    Requests current orderbook for the symbols with a depth of 100 which is max.
    Returns a dataframe and flag variable which is indicating if the quote was sent as reversed or not.
    Dataframe 
    """
    pd.DataFrame([quote_symbol, base_symbol], columns = ['symbols']).to_csv('tmp2.csv')

    df1 = symbols_df['ticker'].str.contains(str(base_symbol))
    df2 = symbols_df['currency'].str.contains(str(quote_symbol))
    ticker_df = symbols_df[df2 & df1]

    if ticker_df.empty:
        # print('tickers not found') ##error 4
        raise SymbolError
         
    reversed_order_flag = ticker_df['Reverse_Quote'].values[0] ## bu flag variableını orderbook hesaplamasında ve requestte kullanacağız.

    if reversed_order_flag == 1:
        base_symbol, quote_symbol = quote_symbol, base_symbol

    ## part 3, ftx servisinden orderbookları çekme

    api_link = f'https://ftx.com/api/markets/{base_symbol}/{quote_symbol}/orderbook?depth=100'
    response_orderbook = requests.get(api_link)
    response_data = response_orderbook.json()
    response_message = [*response_data] ## same as dict.keys() but more cpu-efficient according to a stackoverflow comment

    if response_message[0] != 'success':
        raise MarketError  ## error 5

    if response_message[1] != 'result': 
        raise OrderbookError(response_data['error'])
        print('orderbook request responded an error, error message:')
        print(response_data['error']) ##error 6

    ## part 4, hesaplamalar

    response_column_names = ["price", "qty"]

    ## orderbook ana dataframei. Sort ve asks/bids ayrımı var. Cumulative sumla işlem yapıldığı için sort sırası önemli.
    if action == 'buy':
        df = pd.DataFrame(response_data['result'].get('asks'), columns = response_column_names).sort_values(by='price', ascending=True)
    else:
        df = pd.DataFrame(response_data['result'].get('bids'), columns = response_column_names).sort_values(by='price', ascending=False)

    ## inverse orderlar için yapılan dönüşüm
    ### qty --> kaç dolarlık pasif order var
    ### price --> birim dolarlık fiyat
    #df_initial = df.copy()

    if reversed_order_flag == 1:
        df['qty'] = df['qty'] * df['price']
        df['price'] = 1 / df['price']

    df['cumsumqty'] = df['qty'].cumsum()
    
    return df, reversed_order_flag   ##returns one sided orderbook

    ## full orderbook
    # bids = pd.DataFrame(response_data['result'].get('bids'), columns = ['price', 'bids'])
    # asks = pd.DataFrame(response_data['result'].get('asks'), columns = ['price', 'asks'])
    # full_orderbook = asks.merge(bids, how='outer', on='price').loc[:,['bids', 'price', 'asks']].sort_values('price', ascending=False)
    


def market_order(action, base_currency, quote_currency, amount):


    if action not in ['buy', 'sell']:
        return {'InputError': 'action value is invalid.'}

    if not isinstance(amount, Number):
        return {'InputError': 'amount value is not a number.'}

    try:
        symbols_df = active_symbols()
    except MarketError:
        return {'Error1' : 'Initial API Request Failed while fetching market info.'}
    
    try: 
        orderbook_df, reversed_order_flag = get_orderbook(symbols_df = symbols_df
                                , base_symbol = base_currency
                                , quote_symbol = quote_currency
                                , action = action)
    except SymbolError:
        return {'Error4': 'Tickers do not exist on the market.'}
    except OrderbookError as e:
        return {'OrderbookError': '{}'.format(e.replied_error)}
        
    ## case1: girilen emir miktarı orderbook servisinden dönen miktardan fazla ise orderbookta oldugu kadarini satisfy ederim kalani havada kalacak...
    if orderbook_df['cumsumqty'].max() < amount:
        print('orderbook limit is reached')
        unsatisfied_amount = amount - orderbook_df['cumsumqty'].max()
        amount = orderbook_df['cumsumqty'].max()

    """
    AVG Price hesaplama ve order qty'nin eşleşeceği/kapatacaği orderlari buluyoruz
    1. Kaçinci order'a kadar gidecek, index bulma
    2. Bu ordera kadarki olan orderlari çekme
    3. Son satirdaki orderla beraber kümüle pasif quote qty miktari order miktarindan fazla ise bir kismini biracakaği için düzeltme
    4. Cumsum yeniden hesaplaniyor ve orderin alabildiği miktari getiriyor
    5. AvgPrice hesaplaniyor, agirlikli ortalama
    6. Inverse order degil ise fiyati 2 haneye yuvarliyorum.
    """

    filter_index = orderbook_df[orderbook_df['cumsumqty'] >= amount].index.min() + 1 ## son satırı partially 
    subset_orderbook_df = orderbook_df.iloc[0:filter_index,:].copy() 
    subset_orderbook_df.loc[filter_index - 1,'qty'] = amount - (subset_orderbook_df.loc[filter_index-1,'cumsumqty'] - subset_orderbook_df.loc[filter_index-1,'qty'])
    subset_orderbook_df['cumsumqty'] = subset_orderbook_df['qty'].cumsum()
    avg_price = subset_orderbook_df.apply(lambda row: row['price'] * row['qty'], axis = 1).sum() / subset_orderbook_df['qty'].sum()
    if reversed_order_flag == 0:
        avg_price = round(avg_price,2)

    #return df, subset_df, avg_price, df_initial
    return {'total':amount, 'price':avg_price, 'currency': quote_currency}


def limit_order(action, base_currency, quote_currency, amount, price, iceberg = 1):
    """
    Limit order request, Returns dict    
    """

    if action not in ['buy', 'sell']:
        return {'InputError': 'action value is invalid.'}

    if not isinstance(amount, Number):
        return {'InputError': 'amount value is not a number.'}

    try:
        symbols_df = active_symbols()
    except MarketError:
        return {'Error1' : 'Initial API Request Failed while fetching market info.'}

    try: 
        orderbook_df, reversed_order_flag = get_orderbook(symbols_df = symbols_df
                                , base_symbol = base_currency
                                , quote_symbol = quote_currency
                                , action = action)
    except SymbolError:
        return {'Error4': 'Tickers do not exist on the market.'}
    except OrderbookError as e:
        return {'OrderbookError': '{}'.format(e.replied_error)}

    market_best_price = orderbook_df.loc[0,'price']

    # burda 2 tane if var, market fiyatından otomatik execute olacak limit orderlar için, price aşağı/yukarı girildiyse ifler çalışacak
    ## Marketta bekleyen pasif orderların fiyatı limit orderda verilen price'a getirene kadar veya amount bitene kadar süpürüyor

    ### best bid priceın üzerine atılan buy limit orderlar için
    if action == 'buy':
        if price > market_best_price:

            filter_index = orderbook_df[(orderbook_df['cumsumqty'] <= amount) & (orderbook_df['price'] <= price)].index.max() + 1 ## son satırı partially 
            subset_df = orderbook_df.iloc[0:filter_index,:].copy()         
            
            if subset_df.tail(1)['cumsumqty'].item() > amount:
                subset_df.loc[filter_index - 1,'qty'] = amount - (subset_df.loc[filter_index-1,'cumsumqty'] - subset_df.loc[filter_index-1,'qty'])
                subset_df['cumsumqty'] = subset_df['qty'].cumsum()

            avg_price = subset_df.apply(lambda row: row['price'] * row['qty'], axis = 1).sum() / subset_df['qty'].sum()

            if reversed_order_flag == 0:
                avg_price = round(avg_price,2)        

            fulfilled_qty = subset_df.tail(1)['cumsumqty'].item()            
            remaining_qty = amount - fulfilled_qty
            
            dict_executed = {'Total':fulfilled_qty, 'Price':avg_price, 'Currency': quote_currency}
            dict_base_limitorders = {'Order_size' : remaining_qty/iceberg, 'Price' : price, 'Currency' : quote_currency}            
            return_dict_limitorders = [dict_base_limitorders for i in range(iceberg)]
            
            #{i: dict_base_limitorders for i in range(iceberg)}
            

            return { 'Limit Order' : return_dict_limitorders,
                      'Executed Order' : dict_executed}

    ### best ask priceın altına atılan sell limit orderlar için
    if action == 'sell':
        if price < market_best_price:

            filter_index = orderbook_df[(orderbook_df['cumsumqty'] <= amount) & (orderbook_df['price'] >= price)].index.max() + 1 ## son satırı partially 
            subset_df = orderbook_df.iloc[0:filter_index,:].copy()         
            
            if subset_df.tail(1)['cumsumqty'].item() > amount:
                subset_df.loc[filter_index - 1,'qty'] = amount - (subset_df.loc[filter_index-1,'cumsumqty'] - subset_df.loc[filter_index-1,'qty'])
                subset_df['cumsumqty'] = subset_df['qty'].cumsum()

            avg_price = subset_df.apply(lambda row: row['price'] * row['qty'], axis = 1).sum() / subset_df['qty'].sum()
            if reversed_order_flag == 0:
                avg_price = round(avg_price,2)        

            fulfilled_qty = subset_df.tail(1)['cumsumqty'].item()            
            
            dict_executed = {'Total':fulfilled_qty, 'Price':avg_price, 'Currency': quote_currency}

            remaining_qty = amount - fulfilled_qty

            dict_base_limitorders = {'Order_size' : remaining_qty/iceberg, 'Price' : price, 'Currency' : quote_currency}            
            return_dict_limitorders = [dict_base_limitorders for i in range(iceberg)] 
            #{i: dict_base_limitorders for i in range(iceberg)}
            
            return { 'Limit Order' : return_dict_limitorders,
                      'Executed Order' : dict_executed}

    ### normal caseler için
    dict_base_limitorders = {'Order_size' : amount/iceberg, 'Price' : price, 'Currency' : quote_currency}  
    return_dict = [dict_base_limitorders for i in range(iceberg)]
    return return_dict
