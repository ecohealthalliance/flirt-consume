import data
import pandas as pd
from settings_dev import host, db 
from pymongo import IndexModel, MongoClient, ASCENDING
from datetime import datetime, timedelta
import os.path
import argparse
import time
from dateutil import tz

uri = 'mongodb://%s/%s' % (host, db)
client = MongoClient(uri)
db = client[db]
parser = argparse.ArgumentParser()
args = None

def read_file(datafile, flights=False):
  try:
    bulk = db.legs.initialize_unordered_bulk_op()
    bulk_schedule = db.schedules.initialize_unordered_bulk_op()
    bulk_flights = None
    # will be used in future iteration of consume where we insert individual flights instead of flight schedules 
    def create_flights(record):
      # create range of dates between effective/discontinued dates for this leg
      dates = get_date_range(record)
      arrivalPieces = record.arrivalTimePub.split(":")
      departurePieces = record.departureTimePub.split(":")

      # get arrival timezone - comes in the format "+0200"  First two numbers are the hour and second two are the minute.
      arrivalHour = int(record.arrivalUTCVariance[:3])
      arrivalMinute = int(record.arrivalUTCVariance[3:])
      arrivalOffset = int(record.arrivalUTCVariance)
      tzArrival = tz.tzoffset(None, (arrivalHour * 3600) + (arrivalMinute * 60))
      # get departure timezone
      departureHour = int(record.departureUTCVariance[:3])
      departureMinute = int(record.departureUTCVariance[3:])
      departureOffset = int(record.departureUTCVariance)
      tzDeparture = tz.tzoffset(None, (departureHour * 3600) + (departureMinute * 60))

      # if the arrival time is before the departure time then we need to increment the arrival date.
      increment_arrival_date = (int(arrivalPieces[0] + arrivalPieces[1]) + arrivalOffset) < (int(departurePieces[0] + departurePieces[1]) + departureOffset)

      for date in dates:
        # set times and timezone.  If the departure time is AFTER the arrival time we can 
        # assume the flight lands the next day and we need to increment the date
        arrivalDateTime = date.replace(hour=int(arrivalPieces[0]), minute=int(arrivalPieces[1]), tzinfo=tzArrival)
        if increment_arrival_date:
          arrivalDateTime += timedelta(days=1)

        departureDateTime = date.replace(hour=int(departurePieces[0]), minute=int(departurePieces[1]), tzinfo=tzDeparture)

        bulk_flights.insert({
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
      insert_record = {
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
          "calculatedDates": get_date_range(record),
          "scheduleFileName": os.path.basename(datafile)
        }
      bulk.insert(insert_record)
      bulk_schedule.insert(insert_record)
    print "begin read csv", datafile
    start = time.time()
    data = pd.read_csv(datafile, dtype={'arrivalUTCVariance': str, 'departureUTCVariance': str}, converters={'effectiveDate': convert_to_date, 'discontinuedDate': convert_to_date}, sep=',')
    end = time.time()
    print "finished reading CSV", end - start

    date = data['effectiveDate'].min()
    update_previous_dump(date,flights)

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
        bulk_flights = db.flights.initialize_unordered_bulk_op()
        create_flights(record)
        bulk_flights.execute()
    try:
      bulk.execute()
      bulk_schedule.execute()
    except Exception as e:
      print "Problem bulk executing schedule data:", e
    end = time.time()
    print "done processing records", end - start
  except ValueError as e:
    print e

def get_date_range(record):
  days = [record.day1, record.day2, record.day3, record.day4, record.day5, record.day6, record.day7]
  startDate = record.effectiveDate
  endDate = record.discontinuedDate
  delta = endDate - startDate
  dates = []
  for dateNumber in range(delta.days + 1):
    date = startDate + timedelta(days=dateNumber)
    if days[date.weekday()]:
      dates.append(date)
  return dates

def drop_indexes():
  db.legs.drop_indexes()

def create_indexes():
  idIndex = IndexModel([("_id", ASCENDING)])
  departureIndex = IndexModel([("departureAirport._id", ASCENDING)])
  departEffectDiscIndex = IndexModel([
    ("departureAirport._id", ASCENDING),
    ("effectiveDate", ASCENDING),
    ("discontinuedDate", ASCENDING)
  ])
  effectiveIndex = IndexModel([("effectiveDate", ASCENDING)])
  discontinueIndex = IndexModel([("discontinuedDate", ASCENDING)])
  db.legs.create_indexes([
    idIndex,
    departureIndex,
    departEffectDiscIndex,
    effectiveIndex,
    discontinueIndex
  ])

def convert_to_date(value):
    return datetime.strptime(value, "%d/%m/%Y")

def update_previous_dump(dumpDate, flights=False):
  #update the discontinued date for all of the previous legs to the date of the current datafile
  print "update previous dump", dumpDate
  start = time.time()
  # since the new dump will decide what records exist going foward we remove any 
  # previous records where the effectiveDate strays into the present
  if flights:
    db.flights.delete_many({"departureDateTime": {"$gte": dumpDate}})
  else:
    db.legs.delete_many({"effectiveDate": {"$gte": dumpDate}})  
    # pull array values
    db.legs.update(
      { "effectiveDate": {"$lt": dumpDate},
        "discontinuedDate": {"$gte": dumpDate}
      },
      {"$pull": {"calculatedDates": {"$gte": dumpDate}}},
      upsert=False, 
      multi=True
    )
    db.legs.update(
      { "effectiveDate": {"$lt": dumpDate},
        "discontinuedDate": {"$gte": dumpDate}
      },
      {"$set": {"discontinuedDate": dumpDate - timedelta(days=1)}}
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
    read_file(csv, args.flights)
  print "Re-creating indexes..."
  start = time.time()
  create_indexes()
  end = time.time()
  print "Indexes re-created!", end - start
