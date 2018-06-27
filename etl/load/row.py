import psycopg2
from bson.json_util import loads, dumps

from etl.load import init_pg as pg, table
from etl.monitor import logger

from psycopg2.extras import execute_values

# Open a cursor to perform database operations
def insert(db, schema, table, attrs, values):
  """
  Inserts a row defined by attributes and values into a specific 
  table of the PG database.

  Parameters
  ----------
  table_name : string
  attrs :     string[]
  values :    string[]

  Returns
  -------
  -

  Example
  -------
  insert('Audience', [attributes], [values])
  """
  temp = []
  for v in values:
    if type(v) is list:
      if type(v[0]) is str and v[0].startswith("{"):
        temp.append('array[%s]::jsonb[]')
        continue
      temp.append('%s')      

    else:
      temp.append('%s')

  temp = ', '.join(temp)
  attrs = ', '.join(attrs)

  cmd = "INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT DO NOTHING;" % (schema, table.lower(), attrs, temp)
  
  # MoSQL ignores the document and logs a warning
  # if a document could not be inserted.
  # We will decide later what to do with DataErrors.
  try:
    db.execute_cmd(cmd, values)
  except Exception as ex:
    logger.error("[ROW] Insert failed: %s", ex)

def insert_bulk(db, schema, table, attrs, values):
  """
  Inserts a row defined by attributes and values into a specific 
  table of the PG database.

  Parameters
  ----------
  table_name : string
  attrs :     string[]
  values :    string[]

  Returns
  -------
  -

  Example
  -------
  insert('Audience', [attributes], [values])
  """
  temp = []
  for a in attrs:
    temp.append('%s')      

  temp = ', '.join(temp)
  # needed for upsert
  excluded = [('EXCLUDED.%s' % a) for a in attrs]
  attrs_reduced = [('"%s"' % a) for a in attrs]
  attrs_reduced = ', '.join(attrs_reduced)
  attrs = [('"%s"' % a) for a in attrs]
  attrs = ', '.join(attrs)
  excluded = ', '.join(excluded)
  
  # default primary key in Postgres is name_of_table_pkey
  constraint = '%s_pkey' % table
  cmd = "INSERT INTO %s.%s (%s) VALUES %s ON CONFLICT ON CONSTRAINT %s DO UPDATE SET (%s) = ROW(%s);" % (schema, table.lower(), attrs, '%s', constraint, attrs_reduced, excluded)
  # MoSQL ignores the document and logs a warning
  # if a document could not be inserted.
  # We will decide later what to do with DataErrors.
  try:
    execute_values(db.cur, cmd, values)
    db.conn.commit()
  except Exception as ex:
    logger.error("[ROW] Bulk insert failed: %s" % ex)

def update(db, schema, table_name, attrs, values):
  """
  Updates a row in a specific table of the PG database.

  Parameters
  ----------
  table_name : string
  attrs :     string[]
  values :    string[]

  Returns
  -------
  -

  Example
  -------
  update('audience', [attributes], [values])

  """
  attr_val_pairs = []

  oid = ""
  nr_of_attrs = len(attrs)
  
  if nr_of_attrs < 2:
    return 
  for i in range(len(attrs)):
    pair = ""
    if attrs[i] == "id":
      oid = "'%s'" % str(values[i])
      continue
    if type(values[i]) is str:
      if values[i].startswith("{") is True:
        pair = "%s = '%s'" % (attrs[i], values[i])      
      pair = "%s = '%s'" % (attrs[i], values[i])
    else:
      pair = "%s = %s" % (attrs[i], values[i])
    attr_val_pairs.append(pair)
      
  pairs = ", ".join(attr_val_pairs)
  cmd = "UPDATE %s.%s SET %s WHERE _id = %s;" % (schema, table_name.lower(), pairs, oid)
  logger.info("[ROW] Updated record from table %s: [id = %s]." % (table_name.lower(), oid))
  try:
    db.execute_cmd(cmd)
  except Exception as ex:
    logger.error('[ROW] Update failed: %s' % ex)

def delete(db, schema, table_name, oid):
  """
  Deletes a row in a specific table of the PG database.

  Parameters
  ----------
  table_name : string
  object_id : ObjectId
              (will need to get the hex encoded version of ObjectId with str(object_id))

  Returns
  -------
  -

  Example
  -------
  delete(db, schema, 'Audience', ObjectId("5acf593eed101e0c1266e32b"))

  """
  cmd = "DELETE FROM %s.%s WHERE id='%s';" % (schema, table_name.lower(), oid)
  try:
    db.execute_cmd(cmd)
  except Exception as ex:
    logger.error("[ROW] Delete failed: %s" % ex)
