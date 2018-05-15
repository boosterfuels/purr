import bson
import datetime

def get_pg_type(item):
  """
  Returns the item with its PG type.
  In case of list it detects if it consist of primitive types.
  If yes, it will return the corresponding type.
  Otherwise it will be a json array.
  Also, if a string is found, the ' will be replaced with '' so PG parser
  can know it's an apostrophe.
  """
  pg_type = None
  item_type = type(item)
  
  if item_type is bool:
    pg_type = 'boolean'
  
  elif item_type is int:
    pg_type = 'integer'

  elif item_type is float:
    pg_type = 'float'  

  elif item_type is str:
    pg_type = 'text'
    item = item.replace("'", "''")

  elif item_type is datetime.datetime:
    pg_type = 'timestamp'

  elif item_type is bson.objectid.ObjectId:
    pg_type = 'varchar(80)'
    
  elif item_type is dict:
    pg_type = 'jsonb'
  
  # TODO create an array for this in the future
  # lists should be mapped as ARRAY
  # dict should be hstore
  elif item_type is list:
    pg_type = get_list_type(item)

  elif item_type is type(None):
    item = 'null'
    
  else:
    pg_type = 'jsonb'

  return item, pg_type

def get_list_type(curr_list):
  """
  Determines the type of the elements in the array.
  Default is json[].
  integer[]
  """
  lt = None
  if len(curr_list) > 0:
    curr = curr_list[0]
    if type(curr) is str:
      lt = 'text[]'
    elif type(curr) is int:
      lt = 'integer[]'
    elif type(curr) is bson.objectid.ObjectId:
      lt = 'text[]'
    elif type(curr) is dict:
      lt = 'json[]'

  
  return lt

def rename(name_old, type_orig, type_new):
  # print("TYPE ORIG", name_old, type_orig, type_new)
  name_new = None
  if type_equal(type_orig, type_new) is True:
    return name_new
  elif type_new == 'text':
    if type_orig not in ['character', 'char(24)', 'text']:
      name_new = name_old + "_t"
  elif type_new == 'float':
    name_new = name_old + "_f"
  elif type_new == 'boolean':
    name_new = name_old + "_b"
  elif type_new == 'integer':
    name_new = name_old + "_i"

  
  return name_new

def type_equal(old, new):
  if('char' in old and 'char' in new) or (old == 'array' and new == 'json[]') or (old == 'double precision' and new == 'float'):
    return True
  return False

def is_nan(x):
  return x != x
