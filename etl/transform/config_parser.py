import yaml
import etl.monitor

colls = {}

# read file
def file_to_dict(path):
  try:
    with open(path, 'r') as stream:
      try:
        conf_file = yaml.load(stream)
        if not conf_file:
          monitor.logging.error('Config file is empty.')
          return None
        return conf_file
      except yaml.YAMLError as exc:
        print(exc)
  except FileNotFoundError as err:
    monitor.logging.error('File not found.')


def read_collection_config():
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
