import yaml

def file_to_dict(path):
  """
  Read a YAML file which contains the following:
  Settings for postgres: 
  db_name: name of database we want to connect to 
  user: name of the database user
  schema_name: name of schema where the collections will be transfered
  schema_reset: drop and create the schema
  table_truncate: truncate tables before transfer
  table_drop: drop tables before transfer
  Settings for mongo: 
  db_name: name of the database we want to connect to
  repl_set_members: connection string; leave out if you want to connect to your local Mongo instance
  collections: list of collections we want to transfer (case sensitive!)
  tailing: option to start tailing the oplog (true or false)
  typecheck_auto: option to determine type for each attribute without defining a config file
  """
  try:
    with open(path, 'r') as stream:
      conf_file = yaml.load(stream)
      if not conf_file:
        return None
      return conf_file
  except Exception as ex:
    print("Failed opening setup file: %s" % ex)

def read_collections_config(path):
  """
    Read a YAML file which contains the user-defined relation settings.
    - names of MongoDB collections and its field names
    - corresponding relation and attribute names/types in PG
    - types of extra properties
      - anything that is not defined by the user goes into _extra_props if it is castable to
      the type _extra_props has
  """
  with open(path, 'r') as stream:
    try:
        conf_file = yaml.load(stream)
        colls = conf_file["booster"]
        return colls
    except Exception as ex:
      print("Failed opening collection file: %s" % ex)

def get_details(colls, name):
  attrs_original = []
  attrs_new = []
  types = []
  collection = colls[name]
  relation_name = collection[":meta"][":table"]
  extra_props_type = collection[":meta"][":extra_props"]
  for field in collection[":columns"]:
    for key, value in field.items() :
      if key.startswith(":") is False:
        attrs_new.append(key)
      
      else:
        if key == ":source":
          attrs_original.append(value)
        elif key == ":type":
          types.append(value)
  return attrs_new, attrs_original, types, relation_name, extra_props_type
