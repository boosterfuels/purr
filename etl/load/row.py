import psycopg2
from bson.json_util import loads, dumps

from etl.load import init_pg as pg, table
from etl.monitor import Logger

logger = Logger('collection-transfer.log', 'ROW')

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
    db.cur.execute(cmd, values)
    db.conn.commit()
  except psycopg2.Error as e:
    logger.error(cmd)

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
  print("UPDATE --", attrs)
  print("UPDATE --", values)
  attr_val_pairs = []

  oid = ""
  nr_of_attrs = len(attrs)
  
  if nr_of_attrs < 2:
    return 
  for i in range(len(attrs)):
    pair = ""
    if attrs[i] == "_id":
      oid = "'%s'" % str(values[i])
      continue
    if type(values[i]) is str:
      if values[i].startswith("{") is True:
        pair = "%s = '%s'" % (attrs[i], values[i])      
      pair = "%s = '%s'" % (attrs[i], values[i])
    else:
      pair = "%s = %s" % (attrs[i], values[i])
    print(type(values[i]))
    print(pair)
    attr_val_pairs.append(pair)
      
  pairs = ", ".join(attr_val_pairs)
  print("PAIRS", pairs)
  cmd = ''.join([
    "UPDATE ",
    schema + "." + table_name.lower(), " SET ",
    pairs,
    " WHERE _id = ",
    oid,
    ";"
  ])
  print(cmd)
  logger.info("UPDATE PING")
  try:
    db.cur.execute(cmd)
    db.conn.commit()
  except psycopg2.DataError as e:
    logger.info("".join([cmd, '\n', repr(e)]))

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
  cmd = "DELETE FROM %s.%s WHERE _id='%s'" % (schema, table_name.lower(), oid)
  logger.info("DELETE PING")
  try:
    db.cur.execute(cmd)
    db.conn.commit()
  except:
    logger.error(cmd)
