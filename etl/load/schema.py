
import psycopg2
from load import pg_init as pg
db = pg.db


def reset(schema_name = 'public'):
  """
  Reset existing schema or create a new one.
  """
  print('Schema is reset.')
  cur = db.cursor()
  cur.execute(' '.join(['DROP SCHEMA IF EXISTS', schema_name, 'CASCADE;']))
  cur.execute(' '.join(['CREATE SCHEMA', schema_name, ';']))
  db.commit()
  cur.close()

def create(schema_name = 'public'):
  """
  Create schema if it does not exist.
  """
  cur = db.cursor()
  cur.execute(' '.join(['CREATE SCHEMA IF NOT EXISTS', schema_name,';']))
  db.commit()
  cur.close()
