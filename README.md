# flirt-consume
Data ingestion project for FLIRT

#### Setup
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

#### Tests
tests are located in the `tests` directory.  To run enter:
```./run_tests.sh```

The shell script does some setup and teardown for the tests.

#### Running

To run data import: `python process.py`

This will pull down all CSV files in the FTP directory and import them.