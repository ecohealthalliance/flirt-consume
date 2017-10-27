"""
Tests the data import process for correct insert and delete behavior across multiple import files.
"""
from __future__ import print_function
import unittest
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import process
from settings_dev import host, db
import pymongo
from datetime import datetime, timedelta

uri = 'mongodb://%s/%s' % (host, db)
client = pymongo.MongoClient(uri)
db = client[db]

class TestDataImport(unittest.TestCase):

  def setup(self):
    print("setup")

  """
  when inserting the first set of data, all rows should appear with the values from the CSV
  """
  def test_first_import(self):
    print("test first import")
    dir_path = os.path.dirname(os.path.realpath(__file__))
    dataFile = dir_path + "/data/EcoHealth_20170207.csv"
    process.read_file(dataFile)
    # assert there are 4 records inserted
    totalLegs = db.legs.count()
    self.assertEqual(4, totalLegs)
    # look at the first record and make sure the correct values were inserted
    leg = db.legs.find().sort('effectiveDate',pymongo.ASCENDING)[0]
    self.assertEqual(leg["flightNumber"], 3001)
    self.assertEqual(leg["totalSeats"], 187)
    self.assertEqual(leg["day1"], True)
    self.assertEqual(leg["day2"], True)
    self.assertEqual(leg["day3"], True)
    self.assertEqual(leg["day4"], True)
    self.assertEqual(leg["day5"], True)
    self.assertEqual(leg["day6"], True)
    self.assertEqual(leg["day7"], False)
    self.assertEqual(leg["carrier"], "0B!")
    self.assertEqual(leg["arrivalAirport"]["_id"], "CLJ")
    self.assertEqual(leg["arrivalAirport"]["city"], "Cluj-Napoca")
    self.assertEqual(leg["departureAirport"]["_id"], "OTP")
    self.assertEqual(leg["departureAirport"]["city"], "Bucharest")
    self.assertEqual(leg["effectiveDate"], datetime.strptime("02/06/2017", "%m/%d/%Y"))
    self.assertEqual(leg["discontinuedDate"], datetime.strptime("03/25/2017", "%m/%d/%Y"))

  """
  when inserting the second set of data, all rows from the second set should be present in the 
  database, but rows from the first import that have an effective date after the current import 
  should be deleted
  """
  def test_second_import(self):
    print("test import second")
    dir_path = os.path.dirname(os.path.realpath(__file__))
    dataFile = dir_path + "/data/EcoHealth_20170509.csv"
    process.read_file(dataFile)
    # assert there are 5 records in db.  This will be the case if two records from the previous dump were deleted.
    totalLegs = db.legs.count()
    self.assertEqual(5, totalLegs)
    # assert there are 2 records from the previous dump in the db.  
    date = datetime.strptime(os.path.basename("EcoHealth_20170509.csv"), "EcoHealth_%Y%m%d.csv")
    previousLegs = db.legs.count({"discontinuedDate": {"$lt": date}})
    self.assertEqual(2, previousLegs)
    # assert that the values for a record in the new datafile were inserted correctly
    leg = db.legs.find().sort('effectiveDate',pymongo.DESCENDING)[0]
    self.assertEqual(leg["flightNumber"], 3001)
    self.assertEqual(leg["totalSeats"], 187)
    self.assertEqual(leg["day1"], True)
    self.assertEqual(leg["day2"], True)
    self.assertEqual(leg["day3"], True)
    self.assertEqual(leg["day4"], True)
    self.assertEqual(leg["day5"], True)
    self.assertEqual(leg["day6"], True)
    self.assertEqual(leg["day7"], False)
    self.assertEqual(leg["carrier"], "0B!")
    self.assertEqual(leg["arrivalAirport"]["_id"], "CLJ")
    self.assertEqual(leg["arrivalAirport"]["city"], "Cluj-Napoca")
    self.assertEqual(leg["departureAirport"]["_id"], "OTP")
    self.assertEqual(leg["departureAirport"]["city"], "Bucharest")
    self.assertEqual(leg["effectiveDate"], datetime.strptime("03/26/2018", "%m/%d/%Y"))
    self.assertEqual(leg["discontinuedDate"], datetime.strptime("03/31/2019", "%m/%d/%Y"))
