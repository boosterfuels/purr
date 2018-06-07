import psycopg2
import etl.monitor

class PgConnection():
  """
  TODO
  create a base class for Connection
  """
  def __init__(self, conn_details):
    dbname = conn_details["db_name"]
    user = conn_details["user"]
    host = conn_details["host"]
    password = conn_details["password"]
    port = conn_details["port"]
    try:
      self.conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s port=%s" % (dbname, user, password, host, port))
      self.cur = self.conn.cursor()
    except Exception as ex:
      monitor.logging.error("Could not connect to Postgres.")
      
  def query(self, query):
      try:
        result = self.cur.execute(query)
      except Exception as ex:
        monitor.logging.error('error execting query "{}", error: {}'.format(query, ex))
        return None
      else:
          return result

  def __del__(self):
    self.conn.close()
    self.cur.close()
