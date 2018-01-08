import os

mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
mongo_db_name = os.environ.get("MONGO_DB", "flirt")
url = os.environ.get("FLIGHT_GLOBAL_FTP_URL")
uname = os.environ.get("FLIGHT_GLOBAL_FTP_UNAME")
pwd = os.environ.get("FLIGHT_GLOBAL_FTP_PASSWORD")
