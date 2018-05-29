from bson import ObjectId
from bson.json_util import default
import json

def is_composite_list(item):
  if len(item):
    if type(item[0]) == dict or type(item[0]) == list:
      return True
  return False

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

def cast_prim(column_type, value):
  new_value = None
  column_type = column_type.lower()
  if column_type == 'text':
    new_value = str(value)

  elif column_type == 'double precision':
    new_value = None;
    try:
      new_value = float(str(value))
    except ValueError as e:
      new_value = 'undefined'

  elif column_type == 'jsonb[]':
    new_value = [json.dumps(v, default=default) for v in value]

  elif column_type == 'jsonb':
    new_value = json.dumps(value, default=default)

  elif column_type == 'boolean':
    new_value = bool(value)
    
  return new_value