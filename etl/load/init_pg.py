import psycopg2
import etl.monitor as monitor

class PgConnection():
  """
  TODO
  create a base class for Connection
  """
  def __init__(self, conn_details):
    settings_local = ['db_name', 'user']
    settings_remote = ['db_name', 'user', 'password', 'host', 'port']
    if set(settings_remote).issubset(conn_details):
      cmd = 'dbname=%s user=%s password=%s host=%s port=%s' % (
        conn_details['db_name'],
        conn_details['user'],
        conn_details['password'],
        conn_details['host'],
        conn_details['port']
      )      
    elif set(settings_local).issubset(conn_details):
      cmd = 'dbname=%s user=%s' % (conn_details['db_name'], conn_details['user'])  
    try:
      self.conn = psycopg2.connect(cmd)
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
