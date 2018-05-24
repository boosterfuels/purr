# access MongoDB
import pymongo

class MongoConnection():
  """
  TODO
  - create a base class for Connection
  - put dbname somewhere else
  """
  def __init__(self, dbname):
    client = pymongo.MongoClient()
    try:
      self.conn = client[dbname]
    except:
      monitor.logging.error("Could not connect to MongoDB.")
