
class MarketError(Exception):
    pass

class SymbolError(Exception):
    pass

class OrderbookError(Exception):
    def __init__(self, replied_error):
        self.replied_error = replied_error

