"""
Tests the very simple spread printer.  While the logic of
spread_printer probably doesn't need any tests, this set
of cases verfies that the algorithm and source play nicely

Given that they do play nicely, spread_printer can serve as
a template for future algorithms.
"""

import unittest
import datetime
from pymongo import MongoClient
from zipline.sources.mongo_db_source import mongo_db_source
from zipline.algorithms.spread_printer import spread_printer
from tests.test_settings import settings

# HACK: add series_store to the PYTHON PATh
import sys
sys.path.append("/Users/mgojohn/grazer")
from series_store.series_store import SeriesExplorer, SeriesSaver

class SpreadPrinterTestCase(unittest.TestCase):

	def setUp(self):
		self.source = self._get_source()
		self.algo = spread_printer()

	def _get_source(self):
		# connect to the test database
		connection = MongoClient(settings["mongo_db_uri"])
		db = connection.zipline_test_db
			
		str_timestamp=str(datetime.datetime.utcnow()).replace(".","_")
		states = [{
			"some_product": {
				str_timestamp: "this attribute is unique to this test",
				"bid": 50,
				"ask": 60
			}
		}]

		self.replays = 10
		for replay in range(self.replays):
			states.append({
				"some_product": {
					str_timestamp: "this attribute is unique to this test",
					"bid": 50,
					"ask": 60
				}
			})
			states.append({
				"some_product": {
					str_timestamp: "this attribute is unique to this test",
					"bid": 50,
					"ask": 61
				}
			})
			states.append({
				"some_product": {
					str_timestamp: "this attribute is unique to this test",
					"bid": 50,
					"ask": 61
				}
			})
			states.append({
				"some_product": {
					str_timestamp: "this attribute is unique to this test",
					"bid": 50,
					"ask": 62
				}
			})
			states.append({
				"some_product": {
					str_timestamp: "this attribute is unique to this test",
					"bid": 50,
					"ask": 60
				}
			})

		saver = SeriesSaver(db)
		dummy_time = datetime.datetime(2000,1,3,12,5,0)
		saver.start(states[0], timestamp=dummy_time)
		for state in states:
			dummy_time = dummy_time + datetime.timedelta(0,3600)
			saver.append(state, timestamp=dummy_time)

		self.explorer = SeriesExplorer(db)
		player = self.explorer.filter(products=["some_product"], attributes=[str_timestamp])[0]

		return mongo_db_source(player)

	def tearDown(self):
		self.explorer.delete_all_series()

	def test_count_spread_changes(self):
		"""
		The algorithm's count of changes seen should reflect test data after being run
		"""
		self.algo.run(self.source)
		print self.replays,"replays",self.algo.changes_seen
		self.assertTrue(self.algo.changes_seen == self.replays*3)

	def test_count_spread_changes_initialization(self):
		"""
		The algorithm's count of changes seen should be 0 before running
		"""
		self.assertTrue(self.algo.changes_seen == 0)