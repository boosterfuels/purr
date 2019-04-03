import yaml
import etl.monitor as monitor


def config_basic(path):
    """
    Read a YAML file which contains the basic settings:
    Settings for postgres:
      schema_name: name of schema where the collections will be transfered
      schema_reset: drop and create the schema
      table_truncate: truncate tables before transfer
      table_drop: drop tables before transfer
      connection: connection string
        (example: postgres://127.0.0.1:5432/postgres)
    Settings for mongo:
      db_name: name of the database we want to connect to
      connection: connection string (example: mongodb://localhost:27017)
    tailing: if True, it starts tailing the oplog
    typecheck_auto: let Purr determine the type of each attribute
    without defining a config file (much slower!)
    Note
    ----
    If you decide to go without a setup file, most of the mentioned values
    will default to the most frequent user-choices so you can type less :)
    Default values:
    schema name: public
    schema_reset: false
    table_truncate: false
    table_drop: false
    tailing: false
    typecheck_auto: false

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
        monitor.logging.error(
            "[CONFIG PARSER] Failed to open setup file: %s" % ex)
        raise SystemExit()


def config_collections(path):
    """
      Read the collection map. The collection map is a YAML file which contains
      the user-defined settings for each relation:
      - name of MongoDB database
      - names of MongoDB collections and its field names
      - corresponding relation and attribute names/types in PG
      - types of extra properties
        - anything that is not defined by the user goes into _extra_props if it
        can be casted to the type _extra_props has

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
      TODO
      ----
      Transfer data from multiple databases.
    """
    try:
        with open(path, 'r') as stream:
            conf_file = yaml.load(stream)
            db_names = list(conf_file.keys())
            colls = conf_file[db_names[0]]
            return colls
    except Exception as ex:
        monitor.logging.error(
            "[CONFIG PARSER] Failed to open collection file: %s" % ex)
        raise SystemExit()


def config_collection_names(colls):
    '''
    Get collection names from collection map (YAML file).
    Parameters
    ----------
    colls : dict
          : collections with all the field descriptions
    Returns
    -------
    coll_names : list
               : names of collections which should be transferred
    '''
    coll_names = list(colls.keys())
    if len(coll_names) == 0:
        monitor.logging.error(
            "[CONFIG PARSER] No collections found in the collection map.")
        raise SystemExit()
    return coll_names


def config_fields(colls, name):
    '''
    Get details based on collection.yml in order to prepare rows for pg.
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
        monitor.logging.warn(
            """
            [CONFIG PARSER] Failed to find description of %s in
            collections.yml. Skipping collection.
            """ % name)
        return ([], [], [], [], [])
    relation_name = collection[":meta"][":table"]
    extra_props_type = collection[":meta"][":extra_props"]
    for field in collection[":columns"]:
        for key, value in field.items():
            if key.startswith(":") is False:
                attrs_new.append(key)
            else:
                if key == ":source":
                    attrs_original.append(value)
                elif key == ":type":
                    types.append(value)
    return attrs_new, attrs_original, types, relation_name, extra_props_type
