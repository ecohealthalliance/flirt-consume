# Handles pulling data files from both FTP and the S3 archive
from __future__ import print_function
import settings
import ftplib
import os.path
from datetime import datetime
import data
from threading import Thread
from time import sleep
import sys
import zipfile
from pymongo import IndexModel, MongoClient
import boto3

ftp = ftplib.FTP()
s3 = boto3.resource("s3")
flirt = s3.Bucket("eha-flirt")

def check_ftp():
  connect_to_ftp()
  print("FTP Connected")
  CSVs = read_files()
  print("Files read")
  return CSVs

def pull_from_s3():
  CSVs = []
  data_directory = os.path.join(os.getcwd(), 'data')
  for s3File in flirt.objects.all():
    if s3File.key.startswith("EcoHealth_"):
      fileName = data_directory + "/" + s3File.key
      if os.path.isfile(fileName):
        print("file already exists(skipping download): ", fileName)
      elif s3File.key.startswith("EcoHealth_"):
        print("downloading....", s3File.key, fileName)
        flirt.download_file(s3File.key, fileName)
      csv = extract_file(fileName)
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
  ftp.connect(settings.url)
  ftp.login(settings.uname, settings.pwd)

def sortByModified( a_string ):
    entry_attr = a_string.split(';')
    modified = entry_attr[2].strip()
    return modified

#downloads an unprocessed file from FlightGlobal's FTP 
def download_file(ftp_entry):
  data_directory = os.path.join(os.getcwd(), 'data')
  file_name = ftp_entry.name.strip()
  filepathname = os.path.join(data_directory, file_name)
  if not os.path.isfile(filepathname):
    print("downloading", ftp_entry.name)
    with open(filepathname,'wb') as file_out:
      try:
        thread = Thread(target = threaded_ftp_progress, args = (ftp_entry,filepathname))
        thread.start()
        ftp.retrbinary('RETR %s' % ftp_entry.name, file_out.write)
      except:
        print("Problem downloading file", ftp_entry.name)
        raise
      finally:
        thread.join()
        print("")
        print("Done downloading", ftp_entry.name, "!!!")
        print("**************************************")
  else:
    print("File already exists - skipping download:", filepathname)

  # backup zip to S3 if it's not already there
  objs = list(flirt.objects.filter(Prefix=file_name))
  if len(objs) == 0:
    print("Backing up file to S3:", file_name)
    data = open(filepathname, 'rb')
    flirt.put_object(Key=file_name, Body=data)
  return filepathname

# extracts the CSV file from the FlightGlobal zip file
def extract_file(file_path):
  data_directory = os.path.dirname(file_path)
  zip_ref = zipfile.ZipFile(file_path, 'r')
  if len(zip_ref.namelist()) != 1:
    raise IOError("ERROR: More than one file contained in Zip, should just be one CSV file")
  print("Extracting zip file: %s" % zip_ref.namelist()[0])
  csvfile = zip_ref.extract(zip_ref.namelist()[0], data_directory)
  zip_ref.close()
  print("CSV Extracted:", csvfile)
  return csvfile

def get_ftp_entries():
  ls = []
  ftp.retrlines('MLSD', ls.append) 
  ls.sort( key= sortByModified)
  return ls

def read_files():
  entries = list(get_ftp_entries())
  CSVs = []
  for entry in entries:
    ftp_entry = FtpEntry(entry)
    if ftp_entry.needs_to_be_processed:
      print("Processing file", ftp_entry.name)
      file_path = download_file(ftp_entry)
      csv = extract_file(file_path)
      CSVs.append(csv)
  return CSVs

class FtpEntry:
  # each entry will be an array like the following: ['Type=file;','Size=34;','Modify=20170207103453.870;',' EcoHealth_20170207.md5']
  def __init__(self, entry):
    entry =  entry.split(";")
    self.type = self.__getValue(entry[0])
    self.size = float(self.__getValue(entry[1]))
    self.modify = datetime.strptime(self.__getValue(entry[2]), '%Y%m%d%H%M%S.%f')
    self.name = entry[3].strip()
    self.extension = os.path.splitext(entry[3])[1]
    self.needs_to_be_processed = self.extension == ".zip"

  # extracts the value from the key/value pair. Example: 'Type=file' returns `file`
  def __getValue(self, pair):
    return pair.split("=")[1]
