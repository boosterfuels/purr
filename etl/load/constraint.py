
import psycopg2
from load import pg_init as pg
db = pg.db
import monitor
import sys

# check if schema exists
# create schema
# alter schema
# delete schema (rollback - in case of error)

logger = monitor.Logger('collection-transfer.log', 'CONSTRAINT')

def add_pk(table_name, attr):
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
  logger.warn("PING")
  cmd = " ".join(['ALTER TABLE', table_name.lower(), 'ADD PRIMARY KEY (', attr, ');'])
  try:
    cur = db.cursor()
    cur.execute(cmd)
  except:
    logger.error(cmd)
  db.commit()
  cur.close()