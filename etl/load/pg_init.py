import psycopg2

class PgConnection():
  """
  TODO
  create a base class for Connection
  """
  def __init__(self):
    self.db = psycopg2.connect("dbname=postgres user=anettbalazsics")

pg_conn = PgConnection()
db = pg_conn.db
