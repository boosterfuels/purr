import psycopg2
from bson.json_util import loads, dumps
from load import pg_init as pg, table
from extract import collection
import monitor

db = pg.db

logger = monitor.Logger('ROWS')
# Open a cursor to perform database operations
def insert(table_name, attrs, values):
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
  attrs = ','.join(attrs)
  values = ",".join(values) 
  
  cmd = ''.join(["INSERT INTO ",
    table_name.lower(), "(",
    attrs,
    ") VALUES (",
    values,
    ");"
  ])
  # MoSQL ignores the document and logs a warning
  # if a document could not be inserted.
  # We will decide it later whata to do with DataErrors.
  try:
    db.cursor().execute(cmd)
    db.commit()
  except psycopg2.DataError as e:
    logger.warn("".join([cmd, '\n' + repr(e)]))

def update(table_name, attrs, values):
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
  logger.info(cmd)
  try:
    db.cursor().execute(cmd)
    db.commit()
  except psycopg2.DataError as e:
    logger.warn("".join([cmd, '\n', repr(e)]))


def delete(table_name, object_id):
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
  logger.info(cmd)
  try:
    db.cursor().execute(cmd)
    db.commit()
  except psycopg2.DataError as e:
    logger.warn("".join([cmd, '\n' + repr(e)]))
