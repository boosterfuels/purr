
import psycopg2
from load import pg_init as pg
db = pg.db

# check if schema exists
# create schema
# alter schema
# delete schema (rollback - in case of error)

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
  print('Adding primary key to', table_name)
  cmd = " ".join(['ALTER TABLE ', table_name, ' ADD PRIMARY KEY (', attr, ');'])
  print(cmd)
  cur = db.cursor()
  cur.execute(cmd)
  db.commit()
  cur.close()