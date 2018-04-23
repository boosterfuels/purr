import psycopg2
from load import pg_init as pg
db = pg.db

def create(name):
  """
  Open a cursor to perform database operations
  Create table with specific name.
  Parameters
  ----------
  name : str
  TODO
  ----
  """
  name = name.lower()
  if exists(name) is True:
    print('Column', name, 'already exists.')
    return
  cmd = ''.join(["CREATE TABLE IF NOT EXISTS ", name, "();"])
  
  print('Creating table', name)
  cur = db.cursor()
  cur.execute(cmd)
  db.commit()

def exists(table):  
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
  cmd="SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name=%s"
  cur.execute(cmd, [table.lower()])
  res = cur.fetchall()
  db.commit()
  cur.close()
  if res:
    return True
  else:
    return False

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
  
  print('Truncate table(s)', cmd)
  cur = db.cursor()
  cur.execute(cmd)
  db.commit()
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
  print('DROP TABLE(S)', tables, '\n')
  cmd = ''.join(["DROP TABLE IF EXISTS ", tables, ";"])
  
  cur = db.cursor()
  cur.execute(cmd)
  db.commit()
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

  Todo
  ----
  - first check if column exists
  """
  print('ALTERING', name)
  print('ADD COLUMN', column_name, '\n')
  cmd = ''.join(["ALTER TABLE IF EXISTS ", name, " ADD COLUMN IF NOT EXISTS ", column_name, " ", column_type, ";"])  
  cur = db.cursor()
  cur.execute(cmd)
  db.commit()
  cur.close()

def remove_column(name, columnName):
  print('Removing column')
  cmd = ''.join(["ALTER TABLE IF EXISTS ", name, " DROP COLUMN IF EXISTS ", columnName, ";"])  
  cur = db.cursor()
  cur.execute(cmd)
  db.commit()
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
  cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
  db.commit()
  row = map(list, cur.fetchall())
  tables = []
  for t in row:
    tables.append(t[0])
  cur.close()
  return tables

def column_exists(table, column):
  cmd = ''.join(["SELECT column_name FROM information_schema.columns WHERE table_name='", table, "' AND column_name='", column, "';"])  
  cur = db.cursor()
  print('Checking if column exists')
  cur.execute(cmd)
  rows = cur.fetchone()
  cur.close()
  if rows:
    return True
  return False

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
  cur.execute(cmd)
  db.commit()
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
    print('Creating table', name)
    create(name)