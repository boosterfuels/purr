import pymongo
import time
from extract import collection
from load import table, row, constraint
from transform import relation, config_parser

from datetime import datetime, timedelta
from bson import Timestamp

class Extractor():
  """
  This is a class for extracting data from collections.
  """

  def __init__(self):
    """Constructor for Extractor"""

  def transfer_auto(self, coll_names, truncate, drop):
    """
    Transfer collections using auto typecheck
    TODO
    ----
    replace relation 
    """
    if collection.check(coll_names) is True:
      print('Transfering collections', coll_names)
    else:
      return

    for coll in coll_names:
      start = time.time() 
      r = relation.Relation(coll)
      if table.exists(coll) is True:
        if drop:
          table.drop(coll_names)
          table.create(coll)
        elif truncate:
          table.truncate(coll_names)
        else:
          print("alter schema")
        # TODO: alter schema
      else:
        table.create(coll)
      for doc in collection.get_by_name(coll):
        r.insert(doc)
        if r.has_pk is False and doc['_id']:
          r.add_pk('_id')
      print(round(time.time()-start,4))
        

  def transfer_conf(self, coll_names, truncate, drop):
    """
    Transfer collections using types in config file
    """
    if collection.check(coll_names) is True:
      print('Transfering collections', coll_names)
      if drop:
        table.drop(coll_names)
      elif truncate:
        table.truncate(coll_names)
    else:
      return

    for coll in coll_names:
      (attrs_conf, attrs_old, types) = config_parser.get_details(coll)
      if table.exists(coll) is True:
        # TODO: alter schema
        if truncate is True:
          print("alter schema")
      else:
        print(attrs_conf)
        print(types)
        table.create(coll.lower(), attrs_conf, types)
      r = relation.Relation(coll)
      if 'id' in attrs_conf:
        r.add_pk('id')
      coll_data = collection.get_by_name(coll)
      r.bulk_insert(coll_data, attrs_conf, attrs_old)
      
          