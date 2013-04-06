from zipline.algorithm import TradingAlgorithm

class spread_printer(TradingAlgorithm):
    """
    Prints any changes in the bid/ask spread
    Records the number of changes seen as self.changes_seen
    """
    def initialize(self):
        # takes no arguments
        # contains no transforms

        self.changes_seen = 0
        self.last_bid = None
        self.last_ask = None

    def handle_data(self, data):
        sid = data.keys()[0]
        if self.last_bid == None:
            pass
        elif self.last_bid != data[sid].bid or self.last_ask != data[sid].ask:
            self.changes_seen += 1
        self.last_ask = data[sid].ask
        self.last_bid = data[sid].bid

    def describe_self(self):
        print "Algorithm:"
        print "   Type: Spread Printer"
        print "   Changes Seen:", self.changes_seen