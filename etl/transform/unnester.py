# for now it just creates the string needed to create an INSERT
# jsonb
# jsonb in array
# array in jsonb
from bson import ObjectId
from bson.json_util import default
import json

def is_composite_list(item):
  if len(item):
    if type(item[0]) == dict or type(item[0]) == list:
      return True
  return False

def escape_chars(text):
  """
  Escapes \n `and ' - needed for inserting jsonb
  Params
  ------
  text : string which needs to be cleaned up
         type: str 
  Returns
  -------
  text : new string
         str
  """
  text = text.replace("\n","\\n").replace("`", "")
  text = text.replace("'", "''")
  if '"' in text:
    text = text.replace('"', '\\"')
  return text

def transform_primitive_list(prim_list, list_type):
  """
  Transforms a list containing items which have primitive types (integer, text).
  Params
  ------
  prim_list: list
  list_type: str
  Returns
  -------
  string which is the value to be inserted into the PG table 
  """
  new_prim_list = []
  for p in prim_list:
    # Get a hex encoded version of ObjectId with str(p).
    p = escape_chars(str(p))
    new_prim_list.append("'" + str(p) + "'")

  transformed = ",".join(new_prim_list)
  return "array[" + transformed + "]::" + list_type

def change_object_id(item):
  for k, v in item.items():
    if type(v) is ObjectId:
      item[k] = str(v)
  return item

def cast(column_type, value):
  if column_type == 'varchar(80)':
    value = str(value)

  elif column_type == 'jsonb[]':
    value = [json.dumps(v, default=default) for v in value]

  elif column_type == 'jsonb':
    temp = change_object_id(value)
    value = json.dumps(temp, default=default)

  elif column_type == 'text[]':
    value = [str(v) for v in value]
  
  return value