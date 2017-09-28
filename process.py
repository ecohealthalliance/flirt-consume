import data
import pandas as pd
from settings_dev import host, db 
from pymongo import IndexModel, MongoClient, ASCENDING
from datetime import datetime, timedelta
import datetime
import os.path
import argparse
import time
from dateutil import tz

uri = 'mongodb://%s/%s' % (host, db)
client = MongoClient(uri)
db = client[db]
parser = argparse.ArgumentParser()
args = None

def get_utc_time(time_str, utc_variance):
    [hours, minutes] = map(int, time_str.split(":"))
    utc_variance_sign = int(utc_variance[0] + "1")
    utc_variance_hours = int(utc_variance[1:3])
    utc_variance_minutes = int(utc_variance[3:])
    utc_formater_date = datetime.datetime(2000, 1, 1,
        hours, minutes)
    utc_formater_date -= (utc_variance_sign * timedelta(
        hours=utc_variance_hours,
        minutes=utc_variance_minutes))
    return utc_formater_date.strftime("%H:%M")

def create_leg(record, schedule_file_name):
    departure_airport = db.airports.find_one({"_id": record.departureAirport})
    arrival_airport = db.airports.find_one({"_id": record.arrivalAirport})
    # set the effective date of the current record to today and insert it
    # NOTE - leaving out stops and stop codes.    Will this break existing FLIRT
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
    # create range of dates between effective/discontinued dates for this leg
    dates = get_date_range(record)
    arrival_time_utc = get_utc_time(record.arrivalTimePub, record.arrivalUTCVariance)
    departure_time_utc = get_utc_time(record.departureTimePub, record.departureUTCVariance)

    increment_arrival_date = arrival_time_utc <= departure_time_utc

    for date in dates:
        arrival_date_time = date.replace(
            hour=int(arrival_time_utc.split(':')[0]),
            minute=int(arrival_time_utc.split(':')[1]))
        # assume the flight lands the next day and we need to increment the date
        if increment_arrival_date:
            arrival_date_time += timedelta(days=1)

        departure_date_time = date.replace(
            hour=int(departure_time_utc.split(':')[0]),
            minute=int(departure_time_utc.split(':')[1]))

        yield {
            "carrier": record.carrier,
            "flightNumber": record.flightnumber,
            "departureAirport": record.departureAirport,
            "arrivalAirport": record.arrivalAirport,
            "totalSeats": record.totalSeats,
            "departureDateTime": departure_date_time,
            "arrivalDateTime": arrival_date_time
        }

def read_file(datafile, flights=False):
    try:
        bulk = db.legs.initialize_unordered_bulk_op()
        bulk_schedule = db.schedules.initialize_unordered_bulk_op()
        bulk_flights = None

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
                insert_record = create_leg(record, os.path.basename(datafile))
                bulk.insert(insert_record)
                bulk_schedule.insert(insert_record)
            else:
                # The flights collection will be used in future iteration of consume
                # where we insert individual flights instead of flight schedules
                bulk_flights = db.flights.initialize_unordered_bulk_op()
                for flight in create_flights(record):
                    bulk_flights.insert(flight)
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
        return datetime.datetime.strptime(value, "%d/%m/%Y")

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
