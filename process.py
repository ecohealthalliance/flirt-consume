import data
from legs import Legs
import pandas as pd
from settings_dev import host, db 
import pymongo
from datetime import datetime, timedelta
import os.path
import argparse
import time

uri = 'mongodb://%s/%s' % (host, db)
client = pymongo.MongoClient(uri)
db = client[db]
bulk = db.legs.initialize_ordered_bulk_op()

def read_file(datafile):
  try:
    date = datetime.strptime(os.path.basename(datafile), "EcoHealth_%Y%m%d.csv")
    print "update previous dump", date
    start = time.time()
    update_previous_dump(date)
    end = time.time()
    print "finished updating previous dump", end - start
    print "begin read csv"
    start = time.time()
    data = pd.read_csv(datafile, converters={'effectiveDate': convert_to_date, 'discontinuedDate': convert_to_date}, sep=',')
    end = time.time()
    print "finished reading CSV", end - start
    # data = pd.read_csv(datafile, converters={'effectiveDate': convert_to_date, 'discontinuedDate': convert_to_date}, nrows=10, sep=',')
    # we don't care about records that have more than 0 stops
    data = data.loc[data["stops"] == 0]
    print "begin processing legs"
    start = time.time()
    for index, leg in data.iterrows():
      if index % 10000 == 0:
        end = time.time()
        print "processed", index, "legs in", end - start
      process_leg(leg)
    end = time.time()
    print "done processing legs", end - start
  except ValueError:
    print "Could not parse date from", datafile

def convert_to_date(value):
    return datetime.strptime(value, "%d/%m/%Y")

def update_previous_dump(dumpDate):
  #update the discontinued date for all of the previous legs to the date of the current datafile
  print "update previous dump", dumpDate
  # since the new dump will decide what records exist going foward we remove any 
  # previous records where the effectiveDate strays into the present
  yesterday = dumpDate - timedelta(days=1)
  db.legs.delete_many({"effectiveDate": {"$gt": yesterday}})
  db.legs.update(
    { "effectiveDate": {"$lt": dumpDate},
      "discontinuedDate": {"$gt": dumpDate}
    },
    {"$set": {"discontinuedDate": yesterday}}
  )

def process_leg(leg):
  # print "process", leg
  # if record has stops != 0 then don't process record. AKA if stops == 0 we process the record
  if leg.stops > 0:
    return

  departureAirport = db.airports.find_one({"_id": leg.departureAirport})
  arrivalAirport = db.airports.find_one({"_id": leg.arrivalAirport})

  # related to part 2 of this story: https://www.pivotaltracker.com/story/show/145527963
  # laying the groundwork for inserting individual flights rather than flight schedules
  # break_leg_into_flights(leg)


  # set the effective date of the current record to today and insert it
  # NOTE - leaving out stops and stop codes.  Will this break existing FLIRT
  # print("departureAirport", departureAirport)
  bulk.insert({
      "carrier": leg.carrier,
      "flightNumber": leg.flightnumber,
      "day1": leg.day1 == 1,
      "day2": leg.day2 == 1,
      "day3": leg.day3 == 1,
      "day4": leg.day4 == 1,
      "day5": leg.day5 == 1,
      "day6": leg.day6 == 1,
      "day7": leg.day7 == 1,
      "effectiveDate": leg.effectiveDate,
      "discontinuedDate": leg.discontinuedDate,
      # "departureCity": leg.departureCity,             #are these needed since we are already including the airports?
      # "departuresState": leg.departureState,
      # "departureCountry": leg.departureCountry,
      # "arrivalCity": leg.arrivalCity,
      # "arrivalState": leg.arrivalState,
      # "arrivalCountry": leg.arrivalCountry,
      "departureAirport": departureAirport,
      "arrivalAirport": arrivalAirport,
      "totalSeats": leg.totalSeats
    })

# will be used in future iteration of consume where we insert individual flights instead of flight schedules 
def break_leg_into_flights():
  print("Need to implement break_leg_into_flights")

if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument("-s", "--s3", help="Specify that files should be downloaded from S3", action="store_true")
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
