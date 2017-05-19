#classes used to interact with database
from settings_dev import host, db 
import pymongo

class FlirtDB():
  def __init__(self):
    uri = 'mongodb://%s/%s' % (host, db)
    client = pymongo.MongoClient(uri)
    self.db = client['flirt']
