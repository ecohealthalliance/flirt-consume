# flirt-consume
Data ingestion project for FLIRT

Create a `settings_dev.py` in the base directory that contains the following information:
```
url = 'FlightGlobal ftp address'
uname = 'FlightGlobal FTP user name'
pwd = 'FlightGlobal FTP password'
host = 'localhost'
db = 'grits-net-meteor'
```

Before running restore the `airports` collection to the new FLIRT db:
```
mongorestore --db flirt --collection airports data/airports/airports.bson
```

To run data import: `python process.py`