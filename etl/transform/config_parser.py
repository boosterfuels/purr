import yaml
import etl.monitor as monitor

def config_basic(path):
  """
  Read a YAML file which contains the basic settings:
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
  tailing: option to start tailing the oplog (true or false)
  typecheck_auto: option to determine type for each attribute without defining a config file
  
  Returns
  -------
  conf_file : dict
            : database name, user, schema_name, ... 
  Parameters
  ----------
  path : string
       : path to file
  Example
  -------
  config_basic('path/to/file')

  """
  try:
    with open(path, 'r') as stream:
      conf_file = yaml.load(stream)
      if not conf_file:
        return None
      return conf_file
  except Exception as ex:
    monitor.logging.error("Failed to open setup file: %s" % ex)

def config_collections(path):
  """
    Read a YAML file which contains the user-defined settings for each relation.
    - names of MongoDB collections and its field names
    - corresponding relation and attribute names/types in PG
    - types of extra properties
      - anything that is not defined by the user goes into _extra_props if it is castable to
      the type _extra_props has

    Returns
    -------
    colls : dict
          : collection details 
    Parameters
    ----------
    path : string
         : path to file
    Example
    -------
    config_collections('path/to/file')
  """
  try:
    with open(path, 'r') as stream:
      conf_file = yaml.load(stream)
      colls = conf_file["booster"]
      return colls
  except Exception as ex:
    monitor.logging.error("Failed to open collection file: %s" % ex)

def config_collection_names(colls):
  return list(colls.keys())

def config_fields(colls, name):
  '''
  Returns details based on collection.yml in order to prepare rows for pg.
  Returns
  -------
  attrs_new: names of columns in PG
  attrs_original: names of fields from MongoDB
  types : column types
  relation_name: names of relation in PG
  extra_props_type: type of extra properties (preferred JSONB)

  Parameters
  ----------
  colls : dict
        : collection names with settings from collections.yml
  name : string
       : name of collection
  Example
  -------
  config_fields(collections_with_settings, collection_name)
  '''
  attrs_original = []
  attrs_new = []
  types = []

  if name in colls.keys():
    collection = colls[name]
  else:
    monitor.logging.warn("Failed to find description of %s in collections.yml. Ignoring collection." % name)
    return ([],[],[],[],[])
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
