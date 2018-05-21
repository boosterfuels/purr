# access MongoDB
import pymongo

class MongoConnection():
  """
  TODO
  - create a base class for Connection
  - put dbname somewhere else
  """
  def __init__(self, db_name=""):
    db_name = 'booster'
    client = pymongo.MongoClient()
    self.db = client[db_name]

mongo_conn = MongoConnection()
db = mongo_conn.db  