"""
Piggybacks some test logic to setup mongo-based spread_printer
algorithm
"""
from tests.test_spread_printer import SpreadPrinterTestCase
from tests.test_settings import settings
from zipline.algorithms.spread_printer import spread_printer

class source_maker(SpreadPrinterTestCase):
    def __init__(self):
        self.source = self._get_source()

print "Demonstration of spread_printer:"
tc = source_maker()
source = tc.source
source.describe_self()

algo = spread_printer()
algo.describe_self()
algo.run(source)
algo.describe_self()