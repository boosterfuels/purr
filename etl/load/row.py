import psycopg2
from bson.json_util import loads, dumps
from load import init_pg as pg, table
from extract import collection
import monitor

logger = monitor.Logger('collection-transfer.log', 'ROW')
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
      if v[0].startswith("{"):
        temp.append('array[%s]::jsonb[]')
        continue
      temp.append('%s')      

    else:
      temp.append('%s')

  temp = ', '.join(temp)
  attrs = ', '.join(attrs)

  cmd = "INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT DO NOTHING;" % (schema, table.lower(), attrs, temp)
  
  logger.warn("INSERT PING")
  # MoSQL ignores the document and logs a warning
  # if a document could not be inserted.
  # We will decide later what to do with DataErrors.
  try:
    cur = db.cursor()
    cur.execute(cmd, values)
    db.commit()
  except psycopg2.Error as e:
    logger.error(cmd)
  cur.close()

def update(db, table_name, attrs, values):
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

  object_id = ""
  nr_of_attrs = len(attrs)
  if nr_of_attrs < 2:
    return 
  for i in range(0, nr_of_attrs):
    if(attrs[i] == "_id"):
      object_id = values[i]
      continue
    attr_val_pairs.append(attrs[i] + " = " + values[i])
  
  pairs = ",".join(attr_val_pairs)
  cmd = ''.join([
    "UPDATE ",
    table_name.lower(), " SET ",
    pairs,
    " WHERE _id = ",
    object_id,
    ";"
  ])
  cur = db.cursor()
  logger.info("UPDATE PING")
  try:
    cur.execute(cmd)
    db.commit()
  except psycopg2.DataError as e:
    logger.info("".join([cmd, '\n', repr(e)]))
  cur.close()

def delete(db, table_name, object_id):
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
  delete('Audience', ObjectId("5acf593eed101e0c1266e32b"))

  """
  cmd = ''.join([
    "DELETE FROM ",
    table_name.lower(),
    " WHERE _id = '",
    str(object_id),
    "';"
  ])
  logger.info("DELETE PING")
  cur = db.cursor()
  try:
    cur.execute(cmd)
    db.commit()
  except:
    logger.error(cmd)
  cur.close()

