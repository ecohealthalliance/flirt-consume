import data
from legs import Legs
import pandas as pd
from settings_dev import host, db 
import pymongo
from datetime import datetime, timedelta
import os.path
import argparse
import time

uri = "mongodb://%s/%s" % (host, db)
client = pymongo.MongoClient(uri)
db = client[db]
parser = argparse.ArgumentParser()
args = None

def read_file(datafile):
  try:
    bulk = db.legs.initialize_unordered_bulk_op()
    bulkFlights = db.flights.initialize_unordered_bulk_op()
    # will be used in future iteration of consume where we insert individual flights instead of flight schedules 
    def create_flights(record):
      dayList = [record.day1, record.day2, record.day3, record.day4, record.day5, record.day6, record.day7]
      # create range of dates between effective/discontinued dates for this leg
      # delta = record.discontinuedDate - record.effectiveDate
      dates = get_date_range(record.effectiveDate,record.discontinuedDate)
      for date in dates:
        arrivalPieces = record.arrivalTimePub.split(":")
        arrivalDateTime = date.replace(hour=int(arrivalPieces[0]), minute=int(arrivalPieces[1]))
        departurePieces = record.departureTimePub.split(":")
        departureDateTime = date.replace(hour=int(departurePieces[0]), minute=int(departurePieces[1]))
        bulkFlights.insert({
          "carrier": record.carrier,
          "flightNumber": record.flightnumber,
          "departureAirport": record.departureAirport,
          "arrivalAirport": record.arrivalAirport,
          "totalSeats": record.totalSeats,
          "departureDateTime": departureDateTime,
          "arrivalDateTime": arrivalDateTime
        })

    def create_leg(record):
      departureAirport = db.airports.find_one({"_id": record.departureAirport})
      arrivalAirport = db.airports.find_one({"_id": record.arrivalAirport})
      # set the effective date of the current record to today and insert it
      # NOTE - leaving out stops and stop codes.  Will this break existing FLIRT
      bulk.insert({
          "carrier": record.carrier,
          "flightNumber": record.flightnumber,
          "day1": record.day1 == 1,
          "day2": record.day2 == 1,
          "day3": record.day3 == 1,
          "day4": record.day4 == 1,
          "day5": record.day5 == 1,
          "day6": record.day6 == 1,
          "day7": record.day7 == 1,
          "effectiveDate": record.effectiveDate,
          "discontinuedDate": record.discontinuedDate,
          "departureAirport": departureAirport,
          "arrivalAirport": arrivalAirport,
          "totalSeats": record.totalSeats,
          "calculatedDates": get_date_range(record.effectiveDate, record.discontinuedDate)
        })
    date = None
    try:
      date = datetime.strptime(os.path.basename(datafile), "EcoHealth_%Y%m%d.csv")
    except ValueError as e:
      print "Could not parse date from", datafile
      return

    update_previous_dump(date,args.flights)

    print "begin read csv"
    start = time.time()
    data = pd.read_csv(datafile, converters={'effectiveDate': convert_to_date, 'discontinuedDate': convert_to_date}, sep=',', nrows=1000)
    end = time.time()
    print "finished reading CSV", end - start

    # data = pd.read_csv(datafile, converters={'effectiveDate': convert_to_date, 'discontinuedDate': convert_to_date}, nrows=10, sep=',')
    # we don't care about records that have more than 0 stops
    data = data.loc[data["stops"] == 0]
    print "begin processing records"
    start = time.time()
    for index, record in data.iterrows():
      if index % 10000 == 0:
        end = time.time()
        print "processed", index, "legs in", end - start
      # if record has stops != 0 then don't process record. AKA if stops == 0 we process the record
      if record.stops > 0:
        return

      if not args.flights:
        create_leg(record)
      else:
        bulkFlights = db.flights.initialize_unordered_bulk_op()
        create_flights(record)
        bulkFlights.execute()
    try:
      bulk.execute()
    except pymongo.errors.InvalidOperation as e:
      print "Problem bulk executing schedule data:", e
    end = time.time()
    print "done processing records", end - start
  except ValueError as e:
    print e

def get_date_range(startDate, endDate):
  delta = endDate - startDate
  dates = []
  for dateNumber in range(delta.days + 1):
    dates.append(startDate + timedelta(days=dateNumber))
  return dates

def convert_to_date(value):
    return datetime.strptime(value, "%d/%m/%Y")

def update_previous_dump(dumpDate, flights=False):
  #update the discontinued date for all of the previous legs to the date of the current datafile
  print "update previous dump", dumpDate
  start = time.time()
  # since the new dump will decide what records exist going foward we remove any 
  # previous records where the effectiveDate strays into the present
  yesterday = dumpDate - timedelta(days=1)
  if flights:
    db.flights.delete_many({"departureDateTime": {"$gt": yesterday}})
  else:
    db.legs.delete_many({"effectiveDate": {"$gt": yesterday}})
    db.legs.update(
      { "effectiveDate": {"$lt": dumpDate},
        "discontinuedDate": {"$gt": dumpDate}
      },
      {"$set": {"discontinuedDate": yesterday}}
    )
  end = time.time()
  print "finished updating previous dump", end - start


if __name__ == '__main__':

  parser.add_argument("-s", "--s3", help="Specify that files should be downloaded from S3", action="store_true")
  parser.add_argument("-f", "--flights", help="Only update the individual Flights collection", action="store_true")
  args = parser.parse_args()

  # setup a way to read backlog of files from S3 instead of reading files from FlightGlobal FTP
  CSVs = None
  # if user specified S3 as data source pull from there
  if args.s3:
    print "processing S3"
    CSVs = data.pull_from_s3()
    CSVs.sort()
  else:
    print "processing FTP"
    # check FTP
    CSVs = data.check_ftp()
  # take list of files returned by FTP check and process them
  for csv in CSVs:
    read_file(csv)
