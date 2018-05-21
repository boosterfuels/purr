import psycopg2
import monitor
from transform import config_parser as cp

class PgConnection():
  """
  TODO
  create a base class for Connection
  """
  def __init__(self, dbname, user):
    self.conn = psycopg2.connect("dbname=%s user=%s" % (dbname, user))
    self.cur = self.conn.cursor()
    monitor.logging.error("Could not connect to Postgres.")
      
  def query(self, query):
      try:
          result = self.cur.execute(query)
      except Exception as error:
        monitor.logging.error('error execting query "{}", error: {}'.format(query, error))
        return None
      else:
          return result

  def __del__(self):
    self.conn.close()
    self.cur.close()
