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
    repl_set_members = ''
    try:
      repl_set_members = settings['repl_set_members']
      client = pymongo.MongoClient(repl_set_members)
    except KeyError:
      client = pymongo.MongoClient()
    try:
      self.conn = client[db_name]
    except Exception as ex:
      monitor.logging.error("Could not connect to MongoDB: %s" % ex)
