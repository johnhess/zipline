"""
Tools to generate data sources.
"""
from copy import deepcopy
import pytz

from zipline.gens.utils import hash_args

from zipline.sources.data_source import DataSource
# HACK: add series_store to the PYTHON PATh
import sys
sys.path.append("/Users/mgojohn/grazer")
import series_store.series_store

class mongo_db_source(DataSource):
    """
    TODO: Update to describe actual behavior
    """

    def __init__(self, player, **kwargs):
        # verify that we've been passed a player
        # assert isinstance(player, series_store.SeriesPlayer)
        self.player = player

        # a sid is a product, so we can ask the player what
        # it contains.  We only look at the first one
        self.sid = list(self.player.products())[0]
        self.attributes = self.player.attributes()

        self.start = self.player.first_timestamp
        self.start = self.start.replace(tzinfo=pytz.timezone('US/Eastern'))
        self.end =  self.player.last_timestamp
        self.end = self.end.replace(tzinfo=pytz.timezone('US/Eastern'))

        # Hash_value for downstream sorting.
        self.arg_string = hash_args(player, **kwargs)

    def describe_self(self):
        print "Source:"
        print "   Hash:", self.instance_hash
        print "   Start:", self.start
        print "   End:", self.end
        print "   Single Sid:", self.sid

    @property
    def mapping(self):
        return {
            'dt': (lambda x: x.replace(tzinfo=pytz.utc), 'timestamp'),
            'sid': (lambda x: self.sid, 'timestamp'),
            'bid': (lambda x: int(x), 'bid'),
            'ask': (lambda x: int(x), 'ask'),
            'volume': (lambda x: 1000, 'timestamp')
        }

    @property
    def instance_hash(self):
        return self.arg_string

    @property
    def raw_data(self):
        while True:
            snapshot = self.player.step()
            # print "snapshot acquired:", snapshot
            for sid in [self.sid]:
                sidrow = deepcopy(snapshot[sid])
                sidrow["timestamp"] = snapshot["timestamp"]
                # print "raw row is:", sidrow
                yield sidrow
