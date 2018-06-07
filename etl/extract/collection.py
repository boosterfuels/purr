from etl.monitor import Logger
from etl.extract import init_mongo

logger = Logger('collection-transfer.log', 'COLLECTION')

def get_names(db):
  """
    Get collection names.
    Returns
    -------
    List of strings.
  """
  return db.collection_names(include_system_collections=False)

def check(db, req_colls):
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
    logger.error('You entered more collection names than actually exist in the database.')
  # when loading collections, load them sorted by ObjectId
  # in case an error happens we can continue loading from that moment
  # and output the latest successful item passed to PG 
  try:
    for col in req_colls:
      collection_names.index(col)
    logger.info('Checking collection names: OK')
  except ValueError:
    logger.error("'%s' is not a collection." % col)
    return False

  return True

def get_by_name(db, name):
  try:
    logger.info('Loading data from collection %s.' % name)
    c = db[name]
    bz = c.find()
    return bz.batch_size(30000)
  except:
    logger.error('Loading data from collection %s failed.' % name)
    return []

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
