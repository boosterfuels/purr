
import psycopg2
from load import pg_init as pg
db = pg.db

# check if schema exists
# create schema
# alter schema
# delete schema (rollback - in case of error)

def reset():
  print('Schema is reset.')
  cur = db.cursor()
  cur.execute('DROP SCHEMA public CASCADE;')
  cur.execute('CREATE SCHEMA public;')
  db.commit()
  cur.close()
