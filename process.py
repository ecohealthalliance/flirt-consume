import ftp
from legs import Legs


if __name__ == '__main__':
  # setup a way to read backlog of files from S3 instead of reading files from FlightGlobal FTP
  # check FTP
  CSVs = ftp.check_ftp()
  # take list of files returned by FTP check and process them
  for csv in CSVs:
    print "csv", csv
    legs = Legs(csv)