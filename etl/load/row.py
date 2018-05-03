import psycopg2
from bson.json_util import loads, dumps
from load import pg_init as pg, table
from extract import collection
from transform import relation

db = pg.db
# cur.execute("INSERT INTO product(store_id, url, price, charecteristics, color, dimensions) VALUES (%s, %s, %s, %s, %s, %s)", (1,  'http://www.google.com', '$20', json.dumps(thedictionary), 'red', '8.5x11'))

INSERT = 'i'
UPDATE = 'u'
DELETE = 'd'

def insert_jsonb(tableName, attrs, values):
  cmd = ''.join(["INSERT INTO ", tableName, '(', ','.join(attrs), ") VALUES(%s)"])
  cur = db.cursor()
  cur.execute(cmd, [dumps(values)])
  db.commit()
  cur.close()

# Open a cursor to perform database operations
def insert(tableName, attrs, values):
  """
  Inserts a row defined by attributes and values into a specific 
  table of the PG database.

  Parameters
  ----------
  tableName : string
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
  # for v in values:
  #   v = "'" + str(v) + "'"
  values = ",".join(values) 
  
  cmd = ''.join(["INSERT INTO ",
    tableName, "(",
    attrs,
    ") VALUES (",
    values,
    ");"
  ])
  print(cmd, "\n")

  db.cursor().execute(cmd)
  db.commit()

# attr string[]
# values string[]
# this will be a tricky part
def select(query):
  print('select')

def update(tableName, attrs, values):
  """
  Updates a row in a specific table of the PG database.

  Parameters
  ----------
  tableName : string
  attrs :     string[]
  values :    string[]

  Returns
  -------
  -

  Example
  -------
  update('Audience', [attributes], [values])

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
    tableName, " SET ",
    pairs,
    " WHERE _id = ",
    object_id,
    ";"
  ])
  print(cmd, "\n")
  db.cursor().execute(cmd)
  db.commit()

def delete(tableName, object_id):
  """
  Deletes a row in a specific table of the PG database.

  Parameters
  ----------
  tableName : string
  object_id : ObjectId
              need to get the hex encoded version of ObjectId with str(object_id)

  Returns
  -------
  -

  Example
  -------
  delete('Audience', ObjectId("5acf593eed101e0c1266e32b"))

  """
  cmd = ''.join([
    "DELETE FROM ",
    tableName,
    " WHERE _id = '",
    str(object_id),
    "';"
  ])
  print(cmd, "\n")
  db.cursor().execute(cmd)
  db.commit()
def make_cmd_iud(oper, table_name, doc):
  """
  Choose a command based on what is in the oplog (insert, delete, update).
  """
  r = relation.Relation(table_name)
  if table.exists(table_name) is False:
    table.create(table_name)

  if oper == INSERT:
    r.insert(doc)

  elif oper == UPDATE:
    # find item with the same object id
    r.update(doc)

  elif oper == DELETE:
    r.delete(doc)
