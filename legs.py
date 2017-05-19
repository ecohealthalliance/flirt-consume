import pandas as pd

class Legs:
  def __init__(self, datafile):
    #read csv into dataframe
    #only reading first 10 rows for testing purposes
    self.data = pd.read_csv(datafile, nrows=10)

    #once data is loaded into dataframe, we will immediately begin processing the legs
    self.data.apply(self.process_leg, axis=1)

    # after all records have been processed this datafile will get an entry in the processedFiles collection

  def process_leg(self, record):
    print "begin processing", record
    # if record has stops != 0 then continue
    # if the record has a match in the database.
    # take the existing match and set it's discontinuedDate to yesterday
    # set the effective date of the current record to today and insert it