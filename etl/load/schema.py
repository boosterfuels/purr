
import psycopg2

def reset(db, schema_name = 'public'):
  """
  Reset existing schema or create a new one.
  """
  cur = db.cursor()
  cur.execute(' '.join(['DROP SCHEMA IF EXISTS', schema_name, 'CASCADE;']))
  cur.execute(' '.join(['CREATE SCHEMA', schema_name, ';']))
  db.commit()
  print('Schema %s is reset.' % (schema_name))
  cur.close()

def create(db, schema = 'public'):
  """
  Create schema if it does not exist.
  """
  cur = db.cursor()
  cmd = 'CREATE SCHEMA IF NOT EXISTS %s;' % (schema)
  cur.execute(cmd)
  db.commit()
  cur.close()
