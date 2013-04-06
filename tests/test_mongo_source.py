"""
Tests the mongo_db_source

This should be run from nose so imports work appropriately
"""

import unittest
import datetime
from pymongo import MongoClient
from zipline.sources.mongo_db_source import mongo_db_source
from tests.test_settings import settings

# HACK: add series_store to the PYTHON PATh
import sys
sys.path.append("/Users/mgojohn/grazer")
from series_store.series_store import SeriesExplorer, SeriesSaver

class MongoDBTestCase(unittest.TestCase):

	def setUp(self):
		# connect to the test database
		print "connecting to:", settings["mongo_db_uri"]
		connection = MongoClient(settings["mongo_db_uri"])
		self.db = connection.zipline_test_db
			
		self.str_timestamp=str(datetime.datetime.utcnow()).replace(".","_")
		self.states = [{
			"some_product": {
				self.str_timestamp: "this attribute is unique to this test",
				"bid": 50,
				"ask": 60
			}
		}]
		self.states.append({
			"some_product": {
				self.str_timestamp: "this attribute is unique to this test",
				"bid": 50,
				"ask": 60
			}
		})
		self.states.append({
			"some_product": {
				self.str_timestamp: "this attribute is unique to this test",
				"bid": 50,
				"ask": 61
			}
		})
		self.states.append({
			"some_product": {
				self.str_timestamp: "this attribute is unique to this test",
				"bid": 50,
				"ask": 61
			}
		})
		self.states.append({
			"some_product": {
				self.str_timestamp: "this attribute is unique to this test",
				"bid": 50,
				"ask": 62
			}
		})
		
		self.saver=SeriesSaver(self.db)

		self.saver.start(self.states[0])
		for state in self.states[1:]:
			self.saver.append(state)

		self.explorer = SeriesExplorer(self.db)
		self.player = self.explorer.filter(products=["some_product"], attributes=[self.str_timestamp])[0]

		self.source = mongo_db_source(self.player)

	def tearDown(self):
		pass

	def test_initial_next(self):
		"""
		The datasource should retun a set of k:v pairs reflecting first state
		"""
		first_row = self.source.next()
		self.assertTrue(first_row.bid == self.states[0]["some_product"]["bid"])
		self.assertTrue(first_row.ask == self.states[0]["some_product"]["ask"])

	def test_subsequent_nexts(self):
		"""
		The datasource should retun a set of k:v pairs reflecting each state
		"""
		for state in range(len(self.states)):
			row = self.source.next()
			# print "expecting bid:", self.states[state]["some_product"]["bid"], "and ask:", self.states[state]["some_product"]["ask"]
			self.assertTrue(row.bid == self.states[state]["some_product"]["bid"])
			self.assertTrue(row.ask == self.states[state]["some_product"]["ask"])

	def test_arg_string(self):
		""" The datasource should have a hash 32 characters long
		"""
		self.assertTrue(len(self.source.instance_hash) == 32)
