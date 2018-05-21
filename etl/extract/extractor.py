import pymongo
import time
from extract import collection
from load import table, row, constraint, schema
from transform import relation, config_parser
from datetime import datetime, timedelta
from bson import Timestamp
import monitor

class Extractor():
  """
  This is a class for extracting data from collections.
  """

  def __init__(self):
    """Constructor for Extractor"""
    self.logger = monitor.Logger('performance.log', 'EXTRACTOR')
    
  def transfer_auto(self, coll_names, truncate, drop, pg, mdb_conn, schema_name):
    """
    Transfer collections using auto typecheck
    TODO
    ----
    replace relation 
    """
    if collection.check(coll_names) is False:
      return

    if drop:
      table.drop(pg, schema_name, coll_names)
    elif truncate:
      table.truncate(pg, schema_name, coll_names)
    
    schema.create(pg, schema_name)

    for coll in coll_names:
      start = time.time()
      r = relation.Relation(pg, schema_name, coll)
      table.create(pg, schema_name, coll)
      for doc in collection.get_by_name(coll):
        r.insert(doc)
        if r.has_pk is False and doc['_id']:
          r.add_pk('_id')
      self.logger.info(coll + ': ' + str(round(time.time() - start, 4)) + ' seconds.')
