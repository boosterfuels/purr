import psycopg2
from bson.json_util import loads, dumps
from load import pg_init as pg, table
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
  row: tuple
  Inserted row.

  Example
  -------
  insert(pg.db, 'Audience', [attributes], [values])

  TODO
  ----
  - don't hardcode schema
  - update doc
  - make this work :D
  """
  attrs = ','.join(attrs)
  for v in values:
    v = "'" + str(v) + "'"
  print(values)
  values = ",".join(values) 
  
  print(attrs, len(attrs))
  cmd = ''.join(["INSERT INTO ",
    tableName, "(",
    attrs,
    ") VALUES (",
    values,
    ");"
  ])
  print('\nCOMMAND...\n\n', cmd, '\n')

  db.cursor().execute(cmd)
  db.commit()

# attr string[]
# values string[]
# this will be a tricky part
def select(query):
  print('select')

def update(attrs, values):
  print('update')
  
def delete():
  print('delete')

def make_cmd_iud(oper, table_name, doc):
  """
  Choose a command based on what is in the oplog (insert, delete, update).
  """
  print('making command')
  column_name = 'bazinga'
  column_type = 'jsonb'
  table.create(table_name)

  table.add_column(table_name, column_name, column_type)
  insert_jsonb(table_name, [column_name], doc)
  print(doc)
  if oper == INSERT:
    print('INSERT')

  elif oper == UPDATE:
    # find item with the same object id
    update([], [])

  elif oper == DELETE:
    delete()
  