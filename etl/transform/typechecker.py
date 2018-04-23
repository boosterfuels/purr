"""

"""

import bson
import datetime

def get_pg_type(item):
  pg_type = None
  item_type = type(item)
  print('type(item) =', type(item))
  print('item =', item)
  
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
    pg_type = 'char(24)'
    
  elif item_type is dict:
    pg_type = 'jsonb'
  
  # TODO create an array for this in the future
  # lists should be mapped as ARRAY
  # dict should be hstore
  elif item_type is list:
    pg_type = 'json[]'

  elif item_type is type(None):
    item = 'null'
    
  else:
    pg_type = 'jsonb'

  return item, pg_type
