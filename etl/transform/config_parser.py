import yaml
import monitor

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


def read_collections_config():
  with open("collections.yml", 'r') as stream:
    try:
        conf_file = yaml.load(stream)
        colls = conf_file["booster"]
        return colls
    except yaml.YAMLError as exc:
        print(exc)

def get_details(colls, name):
  attrs_original = []
  attrs_new = []
  types = []
  collection = colls[name]
  relation_name = collection[":meta"][":table"]
  extra_props_type = collection[":meta"][":extra_props"]
  for field in collection[":columns"]:
    for key, value in field.items() :
      # print(key, value)
      if key.startswith(":") is False:
        # print(key)
        attrs_new.append(key)
      
      else:
        if key == ":source":
          attrs_original.append(value)
        elif key == ":type":
          types.append(value)
  return attrs_new, attrs_original, types, relation_name, extra_props_type