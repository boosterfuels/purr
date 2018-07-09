# access MongoDB
import pymongo

class MongoConnection():
  """
  TODO
  - create a base class for Connection
  - put dbname somewhere else
  """
  def __init__(self, settings):
    db_name = settings['db_name']
    connection_string = ''
    try:
      connection_string = settings['connection']
      self.client = pymongo.MongoClient(connection_string)
    except KeyError:
      self.client = pymongo.MongoClient()
    try:
      self.conn = self.client[db_name]
    except Exception as ex:
      monitor.logging.error("Could not connect to MongoDB: %s" % ex)
