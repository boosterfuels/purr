import psycopg2
from load import pg_init as pg
db = pg.db

# check if schema exists
# create schema
# alter schema
# delete schema (rollback - in case of error)

def create(db_name):
  """
  Create database.

  Parameters
  ----------
  db_name : string
            : name of database

  Returns
  -------
  -

  Example
  -------
  create('booster')

  """
  cmd = " ".join(['CREATE DATABASE ', db_name, ';'])
  cur = db.cursor()
  cur.execute(cmd)
  db.commit()
  cur.close()


def drop():
  print('Dropping database')

def alter(options):
  print('Altering database')

def exists(name):
  print('Checking if database exists')
  return True

  
