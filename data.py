# Handles pulling data files from both FTP and the S3 archive

from settings_dev import url, uname, pwd
import ftplib
import os.path
from datetime import datetime
import data
from threading import Thread
from time import sleep
import sys
import zipfile
from settings_dev import host, db 
import pymongo
import boto3

ftp = ftplib.FTP()
s3 = boto3.resource("s3")
flirt = s3.Bucket("eha-flirt")

def check_ftp():
  connect_to_ftp()
  print "read files"
  CSVs = read_files()
  return CSVs

def pull_from_s3():
  CSVs = []
  data_directory = os.path.join(os.getcwd(), 'data')
  for s3File in flirt.objects.all():
    if s3File.key.startswith("EcoHealth"):
      print "downloading....", s3File.key, data_directory + "/" + s3File.key
      flirt.download_file(s3File.key, data_directory + "/" + s3File.key)
      csv = extract_file(data_directory + "/" + s3File.key)
      CSVs.append(csv)
  return CSVs

def threaded_ftp_progress(ftpEntry, path):
  progress = 0
  last_length = 0
  while progress < 99:
    size = os.path.getsize(path)
    progress = (size/ftpEntry.size) * 100
    output = "".join(["Download progress: ", "{0:.2f}".format(progress), "%"])
    last_length = len(output)
    sys.stdout.write('\b' * last_length)
    sys.stdout.write(output)
    sys.stdout.flush()
    sleep(1)

def connect_to_ftp():
  ftp.connect(url)
  ftp.login(uname, pwd)

def sortByModified( aString ):
    entryAttr = aString.split(';')
    modified = entryAttr[2].strip()
    return modified

#downloads an unprocessed file from FlightGlobal's FTP 
def download_file(ftpEntry):
  print "downloading", ftpEntry.name
  data_directory = os.path.join(os.getcwd(), 'data')
  fileName = ftpEntry.name.strip()
  filepathname = os.path.join(data_directory, fileName)
  # try:
  #   fileOut = open(filepathname,'wb')
  # except:
  #   raise IOError("ERROR: Could not open the output file for writing")

  # try:
  #   thread = Thread(target = threaded_ftp_progress, args = (ftpEntry,filepathname))
  #   thread.start()
  #   ftp.retrbinary('RETR %s' % ftpEntry.name, fileOut.write)
  # except:
  #   print "Problem downloading file", ftpEntry.name
  #   raise
  # finally:
  #   thread.join()
  #   print ""
  #   print "Done downloading", ftpEntry.name, "!!!"
  #   print "**************************************"
  # fileOut.close()

  # backup zip to S3 if it's not already there
  objs = list(flirt.objects.filter(Prefix=fileName))
  if len(objs) == 0:
    print "Backing up file to S3:", fileName
    data = open(filepathname, 'rb')
    flirt.put_object(Key=fileName, Body=data)

  return filepathname

# extracts the CSV file from the FlightGlobal zip file
def extract_file(filePath):
  data_directory = os.path.dirname(filePath)
  zip_ref = zipfile.ZipFile(filePath, 'r')
  if len(zip_ref.namelist()) != 1:
    raise IOError("ERROR: More than one file contained in Zip, should just be one CSV file")
  print "Extracting zip file: %s" % zip_ref.namelist()[0]
  csvfile = zip_ref.extract(zip_ref.namelist()[0], data_directory)
  zip_ref.close()
  print "CSV Extracted:", csvfile
  return csvfile

def read_files():
  ls = []
  ftp.retrlines('MLSD', ls.append) 
  ls.sort( key= sortByModified)
  CSVs = []
  for entry in ls:
    ftpEntry = FtpEntry(entry)
    if ftpEntry.needs_to_be_processed:
      print "Processing file", ftpEntry.name
      filePath = download_file(ftpEntry)
      csv = extract_file(filePath)
      CSVs.append(csv)
  return CSVs

class FtpEntry:
  # each entry will be an array like the following: ['Type=file;','Size=34;','Modify=20170207103453.870;',' EcoHealth_20170207.md5']
  def __init__(self, entry):
    # db = data.FlirtDB().db
    uri = 'mongodb://%s/%s' % (host, db)
    client = pymongo.MongoClient(uri)
    flirtDb = client['flirt']
    entry =  entry.split(";")
    self.type = self.__getValue(entry[0])
    self.size = float(self.__getValue(entry[1]))
    self.modify = datetime.strptime(self.__getValue(entry[2]), '%Y%m%d%H%M%S.%f')
    self.name = entry[3]
    self.extension = os.path.splitext(entry[3])[1]
    # we are assuming that zip files that do not have an entry in the "processedFiles" collection need to be processed 
    self.needs_to_be_processed = self.extension == ".zip" and flirtDb.processedFiles.find_one({'fileName': self.name}) == None

  # extracts the value from the key/value pair. Example: 'Type=file' returns `file`
  def __getValue(self, pair):
    return pair.split("=")[1]

# if __name__ == '__main__':
#   connect_to_ftp()
  # db = data.FlirtDB().db
  # print "db", db.legs.find_one()
