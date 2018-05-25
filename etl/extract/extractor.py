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
    
  def transfer_auto(self, coll_names, truncate, drop, pg, mdb, schema_name):
    """
    Transfer collections using auto typecheck
    TODO
    ----
    replace relation 
    """
    if collection.check(mdb, coll_names) is False:
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
      docs = collection.get_by_name(mdb, coll)
      start_docs = start
      for i in range(docs.count()):
        doc = docs[i]
        if (i+1)%1000==0 and i+1>=1000:
          print('Transferred %d documents from collection %s. (%s s)' % (i+1, coll, str(round(time.time() - start_docs, 4))))
          start_docs = time.time()
        if i+1 == docs.count():
          print('Successfully transferred collection %s (%d documents).' % (coll, i+1))
        r.insert(doc)
        if r.has_pk is False and doc['_id']:
          r.add_pk('_id')
      self.logger.info(coll + ': ' + str(round(time.time() - start, 4)) + ' seconds.')
