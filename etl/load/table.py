import psycopg2
import sys
from etl.monitor import logger

def create(db, schema, name, attrs = [], types = []):
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
    for i in range(nr_of_attrs):
      pair = '"%s" %s' % (attrs[i], types[i])
      if attrs[i] == 'id':
        pair = "%s PRIMARY KEY" % pair
      attrs_and_types.append(pair)
  else:
    attrs_and_types = ""
  
  attrs_and_types = ", ".join(attrs_and_types)

  name = name.lower()
  cmd =  "CREATE TABLE IF NOT EXISTS %s.%s(%s);" % (schema, name, attrs_and_types)
  try:
    db.execute_cmd(cmd)
  except Exception as ex:
    logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))

def exists(db, schema, table):  
  """
  Check if a table exists in the PG database.
  
  Parameters
  ----------
  table : string

  Returns
  -------
  True: table exists in the database
  False: otherwise
  """
  cmd = "SELECT table_name FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s';" % (schema, table.lower())
  
  try:
    res = db.execute_cmd_with_fetch(cmd)
    if res:
      return True
    else:
      return False
  except Exception as ex:
    logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))

def truncate(db, schema, tables):
  """
  Parameters
  ----------
  tables: string[]
  Deletes data from 1..* tables.
  TODO
  ----
  Check if table exists before doing anything.
  """
  tables_cmd = [] 
  for t in tables:
    tables_cmd.append('%s.%s' % (schema, t))
  tables_cmd = ','.join(tables_cmd)
  cmd = "TRUNCATE TABLE %s;" % (tables_cmd)
  
  try:
    db.execute_cmd(cmd)   
  except Exception as ex:
    logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))

def drop(db, schema, tables):
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
  tables_cmd = [] 
  for t in tables:
    tables_cmd.append('%s.%s' % (schema, t))
  tables_cmd = ','.join(tables_cmd)

  cmd = "DROP TABLE IF EXISTS %s" % (tables_cmd)
  
  try:
    db.execute_cmd(cmd)   
  except Exception as ex:
    logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))

def add_column(db, schema, table, column_name, column_type):
  """
  Add new column to a specific table.
  Parameters
  ----------
  table : str
        : name of table to alter
  column_name : str
              : name of new column
  column_type : str
              : type of new column 
  Example
  -------
  add_column(db, 'some_integer', 'integer')
  """
  cmd = "ALTER TABLE IF EXISTS %s.%s ADD COLUMN IF NOT EXISTS %s %s;" % (schema, table.lower(), column_name, column_type)  
  logger.warn("[TABLE] Adding new column to table: %s, column: %s, type: %s" % (table.lower(), column_name, column_type))
  try:
    db.execute_cmd(cmd)   
  except Exception as ex:
    logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))

def add_multiple_columns(db, schema, table, attrs, types):
  """
  Add new column to a specific table.
  Parameters
  ----------
  name : str
  column_name : str
  column_type : str

  Example
  -------
  add_multiple_columns(db, ['nyanya', some_integer'], ['text', integer'])
  """
  statements_add = []
  for i, j in zip(attrs, types):
    statements_add.append(' '.join(['ADD COLUMN IF NOT EXISTS', i, j]))
  statements_merged = ', '.join(statements_add) 
  
  cmd = "ALTER TABLE IF EXISTS %s.%s %s;" % (schema, table.lower(), statements_merged)
  logger.warn("[TABLE] Adding multiple columns to table %s %s;" % (table.lower(), statements_merged))
  try:
    db.execute_cmd(cmd)   
  except Exception as ex:
    logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))

def column_change_type(db, schema, table, column_name, column_type):
  """
  Add new column to a specific table.
  Parameters
  ----------
  name : str
  column_name : str
  column_type : str

  Example
  -------
  column_change_type(pg.db, 'some_integer', 'integer')
  """
  expression = ''
  if column_type == 'jsonb':
    expression = 'to_json(%s)' % column_name
  elif column_type == 'double precision':
    expression = 'CAST(%s as double precision)' % column_name

  if len(expression) == 0:
    cmd = "ALTER TABLE %s.%s ALTER COLUMN %s TYPE %s;" % (schema, table.lower(), column_name, column_type)
  else:
    cmd = "ALTER TABLE %s.%s ALTER COLUMN %s TYPE %s USING %s;" % (schema, table.lower(), column_name, column_type, expression)
  logger.warn("[TABLE] ALTER TABLE %s, changing type of column '%s' to '%s'" % (table.lower(), column_name, column_type))

  try:
    db.execute_cmd(cmd)   
  except psycopg2.ProgrammingError as ex:
    logger.error('[TABLE] ProgrammingError: %s when executing command %s.' % (ex, cmd))
  except Exception as ex:
    logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))

def remove_column(db, table, column_name):
  cmd = "ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s;" % (table.lower(), column_name)  
  logger.warn("[TABLE] Removing column '%s' from table '%s' if exists." % (column_name, table.lower()))
  try:
    db.execute_cmd(cmd)
  except Exception as ex:
    logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))

def get_table_names(db, schema):
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
  cmd = "SELECT table_name FROM information_schema.tables WHERE table_schema='%s'" % (schema)
  try:
    res = execute_cmd_with_fetch(cmd)   
    row = map(list, res)
    tables = []
    for t in row:
      tables.append(t[0])
    return tables
  except Exception as ex:
    logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))

def column_exists(db, table, column):
  cmd = "SELECT column_name FROM information_schema.columns WHERE table_name='%s' AND column_name='%s';" % (table.lower(), column)   
  try:
    rows = db.execute_cmd_with_fetch(cmd)
    if rows:
      return True
    return False
  except Exception as ex:
    logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))

def get_column_names_and_types(db, schema, table):
  """
  Get column names and column types of a specific table.
  Parameters
  ----------
  table_name: str  
  Returns
  -------
  List of column names and corresponding types.
  """
  cmd = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='%s' AND table_name = '%s';" % (schema, table.lower())
  try:
    rows = db.execute_cmd_with_fetch(cmd)
    return rows
  except Exception as ex:
    logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))
