# for now it just creates the string needed to create an INSERT
# jsonb
# jsonb in array
# array in jsonb

def decompose_list(items):
  """
  Decomposes list
  """
  elements = []
  from_list = True

  
  for item in items:
    print(item)
    if type(item) is dict:
      chunk = decompose_dict(item, from_list)
      elements.append(chunk)
    else:
      elements.append(item)
  res = ",".join(elements)
  print(res)
  return res

def decompose_dict(item, from_list):
  """
  Decomposes dictionary
  """
  res = []
    
  for key, value in item.items():
      print(key, value, type(key), type(value))
      if(type(value) is dict):
        value = decompose_dict(value, from_list)
      elif(type(value) is list):
        value = decompose_list(value)        
      res.append('"' + str(key) + '": "' + str(value) + '"')
  return "'{" + ",".join(res) + "}'"
 
# def clean_up(item):
#   item = item.replace("'", '"').replace("datetime.datetime(", '').replace(")", '')
#   item = item.replace("ObjectId(", '').replace(")", '')
#   item = item.replace("True", '"true"').replace("False", '"False"').replace('None', '"NULL"').replace('""', "")
#   print("\nITEM",item)
#   return item

def transform_jsonb(item):
  """
  Decide what to do with composite types.
  """
  print('get_value', type(item))
  if type(item) is list:
    print(item)
    return "array[" + decompose_list(item) + "]::json[]"
  
  elif type(item) is dict:
    from_list = False
    return "jsonb(" + decompose_dict(item, from_list) + ")"
