import psycopg2
import sys

from etl.monitor import logger

def add_pk(db, schema, table, attr):
  """
  Adds primary key to a PostgreSQL table.

  Parameters
  ----------
  tableName : string
            : name of table
  attr  : string
        : attr 

  Returns
  -------
  -

  Example
  -------
  add_pk('Audience', 'id')

  """
  logger.warn("[CONSTRAINT] Adding primary key to table %s" % table)
  cmd = 'ALTER TABLE %s.%s ADD PRIMARY KEY (%s)' % (schema, table.lower(), attr)
  try:
    db.cur.execute(cmd)
    db.conn.commit()
  except:
    logger.error("[CONSTRAINT] %s" % cmd)
