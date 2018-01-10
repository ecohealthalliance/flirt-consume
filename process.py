from __future__ import print_function
import data
import pandas as pd
from settings import mongo_uri, mongo_db_name
from pymongo import IndexModel, MongoClient, ASCENDING
from datetime import datetime, timedelta
import datetime
import os.path
import time
import traceback
import sys
import re
import dateutil.parser as date_parser


client = MongoClient(mongo_uri)
db = client[mongo_db_name]

def parse_time(time_str, utc_variance):
  [hours, minutes, seconds] = map(int, time_str.split(":"))
  utc_variance_sign = int(utc_variance[0] + "1")
  utc_variance_hours = int(utc_variance[1:3])
  utc_variance_minutes = int(utc_variance[3:])
  utc_variance_delta = (utc_variance_sign * timedelta(
    hours=utc_variance_hours,
    minutes=utc_variance_minutes))
  return hours, minutes, utc_variance_delta

def get_utc_datetime(time_str, utc_variance, base_date):
  hours, minutes, utc_variance_delta = parse_time(time_str, utc_variance)
  base_date = base_date.replace(
    hour=hours,
    minute=minutes)
  base_date -= utc_variance_delta
  return base_date

def get_utc_time(time_str, utc_variance):
  return get_utc_datetime(
    time_str, utc_variance,
    datetime.datetime(2000, 1, 1)).strftime("%H:%M")

def create_leg(record, schedule_file_name):
  departure_airport = db.airports.find_one({"_id": record.departureAirport})
  arrival_airport = db.airports.find_one({"_id": record.arrivalAirport})
  # set the effective date of the current record to today and insert it
  # NOTE - leaving out stops and stop codes.  Will this break existing FLIRT
  return {
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
    "departureAirport": departure_airport,
    "arrivalAirport": arrival_airport,
    "totalSeats": record.totalSeats,
    "calculatedDates": get_date_range(record),
    "scheduleFileName": schedule_file_name,
    "arrivalTimeUTC": get_utc_time(record.arrivalTimePub,
                                   record.arrivalUTCVariance),
    "departureTimeUTC": get_utc_time(record.departureTimePub,
                                     record.departureUTCVariance)
  }

def create_flights(record):
  carrier = record.carrier
  flightnumber = record.flightnumber
  departureAirport = record.departureAirport
  arrivalAirport = record.arrivalAirport
  totalSeats = record.totalSeats
  dep_hours, dep_minutes, dep_utc_variance_delta = parse_time(
    record.arrivalTimePub, record.arrivalUTCVariance)
  ar_hours, ar_minutes, ar_utc_variance_delta = parse_time(
    record.departureTimePub, record.departureUTCVariance)
  # create range of dates between effective/discontinued dates for this leg
  for date in get_date_range(record):
    arrival_datetime = date.replace(
      hour=ar_hours,
      minute=ar_minutes) - ar_utc_variance_delta
    departure_datetime = date.replace(
      hour=dep_hours,
      minute=dep_minutes) - dep_utc_variance_delta
    if arrival_datetime <= departure_datetime:
      # assume the flight lands the next day
      arrival_datetime += timedelta(days=1)
    yield {
      "carrier": carrier,
      "flightNumber": flightnumber,
      "departureAirport": departureAirport,
      "arrivalAirport": arrivalAirport,
      "totalSeats": totalSeats,
      "departureDateTime": departure_datetime,
      "arrivalDateTime": arrival_datetime
    }

def read_file(datafile, flights=False):
  try:
    print("begin read csv", datafile)
    start = time.time()
    data = pd.read_csv(datafile, dtype={
      'arrivalUTCVariance': str,
      'departureUTCVariance': str}, converters={
        'effectiveDate': convert_to_date,
        'discontinuedDate': convert_to_date}, sep=',')
    end = time.time()
    print("finished reading CSV", end - start)

    dump_start_date = data['effectiveDate'].min()
    # The first few days of flights after the dump's minimum effective date are intentionally not imported
    # because they do not include the full set of flights available in the prior dumps.
    # The missing flights show up as downward spikes in daily flight plots where the dumps start.
    # Data is only omitted when importing data into the flights collection.
    # The issue might only affect the flights collection if it is a result of standardizing
    # departure datetimes to GMT. If it affects the legs and schedules collections, a fix will
    # be much more complicated.
    if flights:
      dump_start_date += timedelta(days=2)
    update_previous_dump(dump_start_date, flights)

    # filter out rows with stops
    data = data.loc[data["stops"] == 0]
    print("begin processing records")
    start = time.time()
    if flights:
      bulk_flights = None
      for index, record in data.iterrows():
        if index % 1000 == 0:
          if bulk_flights:
            bulk_flights.execute()
          bulk_flights = db.flights.initialize_unordered_bulk_op()
          end = time.time()
          print("processed", index, "rows in", end - start)
        for flight in create_flights(record):
          if flight["departureDateTime"] >= dump_start_date:
            flirt["scheduleFileName"] = os.path.basename(datafile)
            bulk_flights.insert(flight)
    else:
      bulk_legs = None
      bulk_schedule = None
      for index, record in data.iterrows():
        if index % 1000 == 0:
          if bulk_legs:
            bulk_legs.execute()
            bulk_schedule.execute()
          bulk_legs = db.legs.initialize_unordered_bulk_op()
          bulk_schedule = db.schedules.initialize_unordered_bulk_op()
          end = time.time()
          print("processed", index, "rows in", end - start)
        insert_record = create_leg(record, os.path.basename(datafile))
        bulk_legs.insert(insert_record)
        bulk_schedule.insert(insert_record)
    end = time.time()
    print("done processing records", end - start)
  except ValueError as e:
    print('\n'.join([str(i) for i in sys.exc_info()]))
    print(e)

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
    return datetime.datetime.strptime(value, "%d/%m/%Y")

def update_previous_dump(dumpDate, flights=False):
  #update the discontinued date for all of the previous legs to the date of the current datafile
  print("update previous dump", dumpDate)
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
  print("finished updating previous dump", end - start)

def parse_csv_name_to_date(csv_name):
  basename = os.path.basename(csv_name)
  date_match = re.match(r"EcoHealth(_|-)([^_]*\d{4}).*", basename).groups()[1]
  return date_parser.parse(date_match, default=datetime.datetime(2000,1,1))

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument("--s3", help="Specify that files should be downloaded from S3", action="store_true")
  parser.add_argument("--flights", help="Update the individual Flights collection", action="store_true")
  parser.add_argument("--skip_imported", help="Skip previously imported dumps", action="store_true")
  parser.add_argument("csvs", nargs="*", help="Paths to specific CSVs to be processed.")
  args = parser.parse_args()
  collection = "flights" if args.flights else "schedules"
  # Omit previously imported schedules.
  imported_files = list(db.importedFiles.find({
    "importComplete": True,
    "collection": collection
  }))
  schedule_files_to_omit = [file["name"] for file in imported_files]

  if args.csvs:
    CSVs = args.csvs
  else:
    # setup a way to read backlog of files from S3 instead of reading files from FlightGlobal FTP
    CSVs = None
    # if user specified S3 as data source pull from there
    if args.s3:
      print("processing S3")
      CSVs = data.pull_from_s3()
    else:
      print("processing FTP")
      # check FTP
      CSVs = data.check_ftp()
  CSVs = sorted(CSVs, key=parse_csv_name_to_date)
  if args.skip_imported:
    # find first unimported CSV and import everything after it
    while len(CSVs) > 0:
      if CSVs[0] not in schedule_files_to_omit:
        db.importedFiles.update({
          "parsedFileNameDate": {
            "$gte": parse_csv_name_to_date(CSVs[0])
          },
          "collection": collection
        }, {
          "$set" : {
            "importComplete": False
          }
        }, upsert=True, multi=True)
        break
      else:
        print("Skipping " + CSVs.pop(0))
  print("CSVs to imprort:")
  print(", ".join(CSVs))
  # take list of files returned by FTP check and process them
  for csv in CSVs:
    db.importedFiles.update({
      "name": csv,
      "collection": collection
    }, {
      "$set" : {
        "importStartTime": datetime.datetime.now(),
        "importerVerion": "0.0.0",
        "parsedFileNameDate": parse_csv_name_to_date(csv),
        "importComplete": False
      }
    }, upsert=True, multi=True)
    read_file(csv, args.flights)
    db.importedFiles.update({
      "name": csv,
      "collection": collection
    }, {
      "$set" : {
        "importFinishTime": datetime.datetime.now(),
        "importComplete": True
      }
    }, upsert=True)
  print("Re-creating indexes...")
  start = time.time()
  create_indexes()
  end = time.time()
  print("Indexes re-created!", end - start)
