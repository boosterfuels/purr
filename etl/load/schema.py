import psycopg2
from etl.monitor import logger

def reset(db, schema = 'public'):
  """
  Reset existing schema or create a new one.
  """
  cmd_drop = 'DROP SCHEMA IF EXISTS %s CASCADE;' % schema
  cmd_create = 'CREATE SCHEMA %s;' % schema
  try:
    db.cur.execute(cmd_drop)
    db.cur.execute(cmd_create)
    db.conn.commit()
  except:
    logger.error("[SCHEMA] Schema reset failed.")
  logger.info("[SCHEMA] Schema %s is reset." % schema)

def create(db, schema = 'public'):
  """
  Create schema if it does not exist.
  """
  cmd = 'CREATE SCHEMA IF NOT EXISTS %s;' % (schema)
  try:
    db.cur.execute(cmd)
    db.conn.commit()
  except:
    logger.error("[SCHEMA] Creating schema with name %s failed." % schema)
    
