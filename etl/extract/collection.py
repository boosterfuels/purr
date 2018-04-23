from extract import mongo_init
db = mongo_init.db

def get_all():
  """
  Prints all collections
  made sense in the beginning xD
  ---
  TODO:
  remove
  """
  collection_names = db.collection_names(include_system_collections=False)
  # when loading collections, load them sorted by ObjectId
  # in case an error happens we can continue loading from that moment
  # and output the latest successful item passed to PG 
  for col in collection_names:
    c = db[col]
    pprint.pprint(col)
    for doc in c.find():
      pprint.pprint(doc)
      pprint.pprint(c.find().count())

def get_names():
  """
    Get collection names.
    Returns
    -------
    List of strings.
  """
  return db.collection_names(include_system_collections=False)

def check(req_colls):
  """
  Parameters
  ----------
  req_colls: 
   - list of requested collection names; 
  
  Example
  -------
  checkCollectionNames(['Tanker'])  
  checkCollectionNames(['Territory', 'FleetOrder']
  """
  collection_names = db.collection_names(include_system_collections=False)
  if(len(req_colls) > len(collection_names)):
    pprint.pprint('You entered more collection names than actually exist.')
  # when loading collections, load them sorted by ObjectId
  # in case an error happens we can continue loading from that moment
  # and output the latest successful item passed to PG 
  try:
    for col in req_colls:
      collection_names.index(col)
    print('Checking collection names: OK')
  except ValueError:

    print(col, 'is not a collection.')
    return False

  return True

def get_by_name(name):
  c = db[name]
  return c.find()

def get_sorted_by_name(name):
  c = db[name]
  return c.find().sort([("updatedAt", -1)])

def get_latest_update(name):
  """
  Example
  -------
  getLatestUpdate('FuelRequest')
  Prerequisite
  ------------
  non-empty coll
  TODO
  check if empty
  """
  c = db[name]
  docs = c.find({}, {'updatedAt': 1, 'ObjectId': 1}).sort([("updatedAt", -1)])
  if docs.count():
    return docs[0]
  else:
    return None

def get_field_names(collection_name):
  """
  Example
  -------
  TODO
  ----
  check if empty
  """
  c = db[collection_name]
  docs = c.find({}, {'updatedAt': 1, 'ObjectId': 1}).sort([("updatedAt", -1)])
  if docs.count():
    return docs[0]
  else:
    return None

def get_fields_op(doc):
  """
  Parameters
  ----------
  An object (JSON) from a document (fields+values).
  Returns
  -------
  Names of fields.
  Example
  -------
  get_fields_op(doc)
  TODO
  ----
  check if empty
  """
  keys = []
  for key, value in doc.items():
    keys.append(key)
  return keys
