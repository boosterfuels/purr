
import psycopg2
import monitor
import sys

logger = monitor.Logger('collection-transfer.log', 'CONSTRAINT')

def add_pk(db, cur, schema, table, attr):
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
  logger.warn("ADD PRIMARY KEY PING")
  cmd = 'ALTER TABLE %s.%s ADD PRIMARY KEY (%s)' % (schema, table.lower(), attr)
  try:
    cur = db.cursor()
    cur.execute(cmd)
  except:
    logger.error(cmd)
  db.commit()
  cur.close()