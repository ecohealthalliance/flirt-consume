import unittest
from process import create_leg, get_utc_time, create_flights
import datetime

class TestRecord():
  def __init__(self):
    self.__dict__ = {
      'day1': True,
      'day2': True,
      'day3': True,
      'day4': True,
      'day5': True,
      'day6': False,
      'day7': False,
      'flightnumber': 456,
      'effectiveDate': datetime.datetime(2011, 1, 1),
      'discontinuedDate': datetime.datetime(2011, 6, 1),
      'departureAirport': 'LAX',
      'arrivalAirport': 'SEA',
      'totalSeats': 123,
      'carrier': 'Test',
      'arrivalTimePub': '04:30',
      'departureTimePub': '19:40',
      'arrivalUTCVariance': '-0900',
      'departureUTCVariance': '+0230',
    }
test_record = TestRecord()

class TestFunctions(unittest.TestCase):

  def test_create_flights(self):
    first_flight = next(create_flights(test_record))
    self.assertDictEqual(first_flight, {
      'arrivalAirport': 'SEA',
      'arrivalDateTime': datetime.datetime(2011, 1, 4, 13, 30),
      'carrier': 'Test',
      'departureAirport': 'LAX',
      'departureDateTime': datetime.datetime(2011, 1, 3, 17, 10),
      'flightNumber': 456,
      'totalSeats': 123
    })

  def test_get_utc_time(self):
    self.assertEqual(get_utc_time('12:00', '-0800'), '20:00')
