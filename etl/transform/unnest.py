# for now it just creates the string needed to create an INSERT
# jsonb
# jsonb in array
# array in jsonb

def decompose_list(items, is_base):
  """
  Decomposes list
  """
  elements = []
  objects = []

  if is_composite_list(items) == False:
    for item in items:
      elements.append('"' + str(item) + '"')

  else:
    for item in items:
      if type(item) is dict:
        chunk = decompose_dict(item, False)
        elements.append(chunk)
      elif type(item) is list:
        chunk = decompose_list(item, False)
        elements.append(chunk)
      else:
        elements.append(str(item))
  res = ""
  if len(items) > 0:
    if type(items[0]) is dict and is_base is True:
      for item in elements:
        objects.append("'" + str(item) + "'")
      res = ",".join(objects)
    else: 
      res = ",".join(elements)
  return res

def decompose_dict(item, is_base):
  """
  Decomposes dictionary
  """
  res = []
  
  for key, value in item.items():
    if type(value) is list:
      value = decompose_list(value, False)
      res.append('"' + key + '": [' + value + "]")

    elif type(value) is dict:
      value = decompose_dict(value, False)        
      res.append('"' + key + '": ' + str(value))

    else:
      val = escape_chars(str(value))
      res.append('"' + key + '":' + '"' + val + '"')

  return "{" + ",".join(res) + "}"
  
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
    print(text)
    text = text.replace('"', '\\"')
    print(text)

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
    new_prim_list.append("'" + str(p) + "'")

  transformed = ",".join(new_prim_list)
  return "array[" + transformed + "]::" + list_type


def transform_composites(item):
  """
  Decide what to do with json[] and jsonb types.
  base: indicates the first call of the function (needed to decide if we have to 
  put single quotes around jsonb)
  Parameters
  item: dict or list
        value which has non-primitive type
  """
  base = True
  if type(item) is list: 
    decomposed = decompose_list(item, base)
    if len(decomposed) == 0:
      return "array[]::json[]"
    return "array[" + decomposed + "]::json[]"
  
  elif type(item) is dict:
    decomposed = decompose_dict(item, base)
    if len(decomposed) == 0:
      return "jsonb()"
    return "jsonb('" + decomposed + "')"
