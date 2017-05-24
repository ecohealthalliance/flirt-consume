import ftp
from legs import Legs
import pandas as pd
from settings_dev import host, db 
import pymongo
from datetime import datetime, timedelta
import os.path

uri = 'mongodb://%s/%s' % (host, db)
client = pymongo.MongoClient(uri)
db = client['flirt']

def read_file(datafile):
  date = datetime.strptime(os.path.basename(datafile), "EcoHealth_%Y%m%d.csv")
  update_previous_dump(date)
  data = pd.read_csv(datafile, converters={'effectiveDate': convert_to_date, 'discontinuedDate': convert_to_date}, nrows=10, sep=',')
  for index, leg in data.iterrows():
    process_leg(leg)

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

  # set the effective date of the current record to today and insert it
  # NOTE - leaving out stops and stop codes.  Will this break existing FLIRT
  # print("departureAirport", departureAirport)
  db.legs.insert_one({
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

if __name__ == '__main__':
  # setup a way to read backlog of files from S3 instead of reading files from FlightGlobal FTP
  # check FTP
  CSVs = ftp.check_ftp()
  # take list of files returned by FTP check and process them
  for csv in CSVs:
    read_file(csv)
    # print "csv", csv
    # legs = Legs(csv)