import pymongo
import time
from extract import collection
from load import table, row, constraint, schema
from transform import relation, config_parser as cp
from datetime import datetime, timedelta
from bson import Timestamp
import monitor
import json
from bson.json_util import default


def is_convertable(type_old, type_new):
  if type_new == 'jsonb':
    return True
  return False

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
      timer_start_docs = start
      nr_of_docs = docs.count()
      for i in range(nr_of_docs):
        doc = docs[i]
        if (i+1)%1000==0 and i+1>=1000:
          print('Transferred %d documents from collection %s. (%s s)' % (i + 1, coll, str(round(time.time() - timer_start_docs, 4))))
          timer_start_docs = time.time()
        if i+1 == docs.count():
          print('Successfully transferred collection %s (%d documents).' % (coll, i+1))
        r.insert(doc)
        if r.has_pk is False and doc['_id']:
          r.add_pk('_id')
      self.logger.info(coll + ': ' + str(round(time.time() - start, 4)) + ' seconds.')

  def transfer_config(self, coll_names, truncate, drop, pg, mdb, schema_name):
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

      coll_settings = cp.read_collections_config()
      for coll in coll_names:
        (attrs_new, attrs_original, types) = cp.get_details(coll_settings, coll)
        start = time.time()
        docs = collection.get_by_name(mdb, coll)
        timer_start_docs = start
        nr_of_docs = docs.count()
        transferring = []
        nr_of_transferred = 1000
        
        r = relation.Relation(pg, schema_name, coll)
        attrs_original = [x.lower().replace("_", "") for x in attrs_original]
        r.create_with_columns(attrs_new, types)
        attrs_new.sort()
        i = 0
        attrs_and_types = dict(zip(attrs_original, types))
        for doc in docs:
          i+=1
          r.insert_config(doc, attrs_new, attrs_and_types)       
          if(i + 1)%nr_of_transferred==0 and i + 1 >= nr_of_transferred:
            print('Transferred %d documents from collection %s. (%s s)' % (i+1, coll, str(round(time.time() -  timer_start_docs, 4) )))
        self.logger.info(coll + ': ' + str(round(time.time() - start, 4)) + ' seconds.')

  def transfer_bulk(self, coll_names, truncate, drop, pg, mdb, schema_name):
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

    coll_settings = cp.read_collections_config()
    for coll in coll_names:
      (attrs_new, attrs_original, types, relation_name, extra_props_type) = cp.get_details(coll_settings, coll)

      start = time.time()
      docs = collection.get_by_name(mdb, coll)

      timer_start_docs = start
      nr_of_docs = docs.count()
      transferring = []
      nr_of_transferred = 10000
      
      r = relation.Relation(pg, schema_name, relation_name)

      # check if there were any changes in the schema
      attrs_types_from_db = table.get_column_names_and_types(pg, schema_name, relation_name)
      attrs_from_db = [x[0] for x in attrs_types_from_db]
      types_from_db = [x[1] for x in attrs_types_from_db]
      # new attributes are fully contained in the attribute list from DB
      if set(attrs_new).issubset(set(attrs_from_db)):
        # check types
        for item in attrs_types_from_db:
          attr_db = item[0]
          type_db = item[1]
          
          if attr_db in attrs_new:
            idx = attrs_new.index(attr_db)
            # type from the db and type from the config file 
            type_old = type_db.lower()
            type_new = types[idx].lower()
            if type_old == 'timestamp without time zone' and type_new == 'timestamp':
              continue
            elif type_old != type_new:
              print("TYPES ARE NOT THE SAME", attr_db, type_old, type_new)
              if is_convertable(type_old, type_new):
                table.column_change_type(pg, schema_name, relation_name, attr_db, type_new)
                print("Type is convertable")
              else:
                table.column_change_type(pg, schema_name, relation_name, attr_db, type_new)
                print("Type is not convertable")                
      else:
        # check old attrs and new ones
        diff = list(set(attrs_new) - set(attrs_from_db))
        print(attrs_types_from_db)
        print(attrs_new)
        print(attrs_from_db)
        print('DIFF', diff)
        # get type of new attributes
        types_to_add = []
        attrs_to_add = []
        for d in diff:
          d_type = types[attrs_new.index(d)]
          attrs_to_add.append(d)
          types_to_add.append(d_type)
        table.add_multiple_columns(pg, schema_name, relation_name, attrs_to_add, types_to_add)
      
      attrs_new.append('_extra_props')
      attrs_original = [x.lower().replace("_", "") for x in attrs_original]
      types.append(extra_props_type)
      r.create_with_columns(attrs_new, types)
      attrs_new.sort()
      i = 0
      attrs_and_types = dict(zip(attrs_original, types))
      transferring = []   
      for doc in docs:
        transferring.append(doc)  
        if (i + 1)%nr_of_transferred==0 and i + 1 >= nr_of_transferred:
          r.insert_config_bulk(transferring, attrs_new, attrs_and_types)
          print('Transferred %d documents from collection %s. (%s s)' % (i+1, coll, str(round(time.time() - timer_start_docs, 4) )))
          transferring = []        
        elif i + 1 == nr_of_docs and ( i + 1 ) % nr_of_transferred != 0:
          r.insert_config_bulk(transferring, attrs_new, attrs_and_types)
          print('Successfully transferred collection %s (%d documents).' % (coll, i+1))
          transferring = []        

        i += 1
      self.logger.info(coll + ': ' + str(round(time.time() - start, 4)) + ' seconds.')
