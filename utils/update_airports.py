import pymongo
import os
import pandas as pd

db = pymongo.MongoClient(
    os.environ.get(
        "MONGO_URI", "mongodb://localhost:27017"))[os.environ.get("MONGO_DB", "flirt")]

for id, row in pd.read_csv("airports.csv").iterrows():
    db_airport = db.airports.find_one(row.id)
    airport_data = {
        "loc": {
            "type": "Point",
            "coordinates": [
                row.lon, row.lat
            ]
        },
        "countryName": row.countryName,
        "city": row.city,
        "globalRegion": row.region
    }
    if not db_airport:
        airport_data["name"] = row.id
    db.airports.update_one({ "_id": row.id }, { "$set" : airport_data }, upsert=True)
