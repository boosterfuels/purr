import yaml
import pprint
colls = {}

# read file
with open("collections.yml", 'r') as stream:
  try:
      conf_file = yaml.load(stream)
      colls = conf_file["booster"]
  except yaml.YAMLError as exc:
      print(exc)

def get_details(relation):
  attrs_original = []
  attrs_new = []
  types = []

  collection = colls[relation]
  for field in collection[":columns"]:
    for key, value in field.items() :
      print(key, value)
      if key.startswith(":") is False:
        print(key)
        attrs_new.append(key)
      
      else:
        if key == ":source":
          attrs_original.append(value)
        elif key == ":type":
          types.append(value)
  return attrs_new, attrs_original, types