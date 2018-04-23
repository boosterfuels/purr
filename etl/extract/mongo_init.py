# access MongoDB
import pymongo

class MongoConnection():
  """
  TODO
  - create a base class for Connection
  - put dbname somewhere else
  """
  def __init__(self):
    client = pymongo.MongoClient()
    self.db = client['booster']

def connect():
  mongo_conn = MongoConnection()
  return mongo_conn.db

db = connect()
  