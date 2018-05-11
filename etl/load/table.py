import psycopg2
from load import pg_init as pg
db = pg.db
import monitor
import sys

logger = monitor.Logger('collection-transfer.log', 'TABLE')

def create(name, attrs = [], types = []):
  """
  Open a cursor to perform database operations
  Create table with specific name.
  Parameters
  ----------
  name : str
  TODO
  ----
  """
  nr_of_attrs = len(attrs)
  attrs_and_types = []
  if nr_of_attrs:
    for i in range(0, nr_of_attrs - 1):
      pair = attrs[i] + " " + types[i]
      attrs_and_types.append(pair)
  else:
    attrs_and_types = ""
  
  attrs_and_types = ", ".join(attrs_and_types)

  name = name.lower()
  cur = db.cursor()
  cmd = ' '.join(["CREATE TABLE IF NOT EXISTS", name, "(", attrs_and_types, ");"])
  logger.warn("PING")
  try:
    cur.execute(cmd)
    db.commit()
  except:
    logger.error(cmd)
  cur.close()

def exists(table_name):  
  """
  Check if a table exists in the PG database.
  
  Parameters
  ----------
  table : string

  Returns
  -------
  True: table exists in the database
  False: otherwise

  Todo
  ----
  don't hardcode schema
  """
  cur = db.cursor()
  cmd = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name='" + table_name.lower()+ "';"
  logger.warn("PING")
  try:
    cur.execute(cmd)
  except:
    logger.error(cmd)

  res = cur.fetchall()
  db.commit()
  cur.close()
  if res:
    return True
  else:
    return False
  # except:
  #   e = sys.exc_info()[0]
  #   logger.warn("".join([cmd, '\n', repr(e)]))

def truncate(tables):
  """
  Parameters
  ----------
  tables: string[]
  Deletes data from 1..* tables.
  TODO
  ----
  Check if table exists before doing anything.
  """
  tables = ','.join(tables)
  cmd = ''.join(["TRUNCATE TABLE ", tables, ";"])
  
  cur = db.cursor()
  logger.warn("PING")
  try:
    cur.execute(cmd)
    db.commit()
  except:
    logger.error(cmd)
  cur.close()

def drop(tables):
  """
  Drop one or more tables in the PG database.

  Parameters
  ----------
  tables : list  

  Example
  -------
  drop(['Hufflepuff', 'test'])

  Todo
  ----
  - first check if all tables in the list exist
  """
  tables = ','.join(tables)
  cmd = ''.join(["DROP TABLE IF EXISTS ", tables, ";"])
  
  cur = db.cursor()
  logger.warn("PING")
  try:
    cur.execute(cmd)
    db.commit()
  except:
    logger.error(cmd)
    # except psycopg2.DataError as e:
  #   logger.warn("".join([cmd, '\n', repr(e)]))
  cur.close()

def add_column(name, column_name, column_type):
  """
  Add new column to a specific table.
  Parameters
  ----------
  name : str
  column_name : str
  column_type : str

  Example
  -------
  add_column(pg.db, 'some_integer', 'integer')
  """
  cmd = ''.join(["ALTER TABLE IF EXISTS ", name.lower(), " ADD COLUMN IF NOT EXISTS ", column_name, " ", column_type, ";"])  
  cur = db.cursor()
  logger.warn("PING")
  try:
    cur.execute(cmd)
    db.commit()
  except:
    logger.error(cmd)
  cur.close()

def add_multiple_columns(name, attrs, types):
  """
  Add new column to a specific table.
  Parameters
  ----------
  name : str
  column_name : str
  column_type : str

  Example
  -------
  add_multiple_columns(pg.db, ['nyanya', some_integer'], ['char(24)', integer'])

  Todo
  ----
  - first check if column exists
  """

  statements_add = []
  for i, j in zip(attrs, types):
    statements_add.append(' '.join(['ADD COLUMN IF NOT EXISTS', i, j]))
  statements_merged = ', '.join(statements_add) 
  
  cmd = ' '.join(["ALTER TABLE IF EXISTS", name.lower(), statements_merged, ";"])  
  cur = db.cursor()
  logger.warn("PING")
  try:
    cur.execute(cmd)
    db.commit()
  except:
    logger.error(cmd)
  cur.close()

def remove_column(name, columnName):
  cmd = ''.join(["ALTER TABLE IF EXISTS ", name.lower(), " DROP COLUMN IF EXISTS ", columnName, ";"])  
  cur = db.cursor()
  logger.warn("PING")
  try:
    cur.execute(cmd)
    db.commit()
  except:
    logger.error(cmd)
  cur.close()

def get_table_names():
  """
  Get existing tables from the PG database.

  Parameters
  ----------
  None
  
  Returns
  -------
  tables: list of strings
  List (strings) of table names of public schema.

  Example
  -------
  exists(pg.db, 'Audience')

  Todo
  ----
  don't hardcode schema
  """
  cur = db.cursor()
  cmd = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
  logger.warn("PING")
  try:
    cur.execute(cmd)
    db.commit()
  except:
    logger.warn(cmd)
  row = map(list, cur.fetchall())
  tables = []
  for t in row:
    tables.append(t[0])
  cur.close()
  return tables

def column_exists(table, column):
  cmd = ''.join(["SELECT column_name FROM information_schema.columns WHERE table_name='", table.lower(), "' AND column_name='", column, "';"])  
  cur = db.cursor()
  logger.warn("PING")
  try:
    cur.execute(cmd)
    rows = cur.fetchone()
    if rows:
      return True
    return False
  except:
    logger.error(cmd)
  cur.close()

def get_column_names_and_types(table_name):
  """
  Get column names and column types of a specific table.
  Parameters
  ----------
  table_name: str  
  Returns
  -------
  List of column names and corresponding types.
  """
  cur = db.cursor()
  cmd = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name = '" + table_name.lower() + "';"
  try:
    cur.execute(cmd)
    db.commit()
  except:
    logger.error(cmd)
  rows = cur.fetchall()
  cur.close()
  return rows

def create_from_oplog(fullname):
  """
  Creates table based on oplog entries.

  Parameters
  ----------
  fullname : combination of schema and table name
  """
  name = fullname.split(".")[1]
  if exists(name) is False:
    create(name)