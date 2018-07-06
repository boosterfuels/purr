import pymongo
import time

from etl.extract import collection
from etl.load import table, row, constraint, schema
from etl.monitor import logger

from etl.transform import relation, config_parser as cp

from datetime import datetime, timedelta
from bson import Timestamp
import json
from bson.json_util import default

class Extractor():
  """
  This is a class for extracting data from collections.
  """

  def __init__(self, pg, mdb, setup_pg, settings, coll_settings):
    """Constructor for Extractor"""

    self.pg = pg
    self.mdb = mdb
    self.schema_name = setup_pg["schema_name"]
    self.typecheck_auto = settings['typecheck_auto']
    self.truncate = setup_pg['table_truncate']
    self.drop = setup_pg['table_drop']
    self.coll_settings = coll_settings
    
  def transfer_auto(self, coll_names):
    """
    Transfers documents or whole collections if the number of fields is less than 30 000 (batch_size).
    Types of attributes are determined using auto typechecking.
    """
    if collection.check(self.mdb, coll_names) is False:
      return

    if self.drop:
      table.drop(self.pg, self.schema_name, coll_names)
    elif self.truncate:
      table.truncate(self.pg, self.schema_name, coll_names)
    
    schema.create(self.pg, self.schema_name)

    for coll in coll_names:
      start = time.time()
      r = relation.Relation(self.pg, self.schema_name, coll)
      table.create(self.pg, self.schema_name, coll)
      docs = collection.get_by_name(self.mdb, coll)
      timer_start_docs = start
      nr_of_docs = docs.count()
      for i in range(nr_of_docs):
        doc = docs[i]
        if (i+1)%1000==0 and i+1>=1000:
          logger.info('[EXTRACTOR] Transferred %d documents from collection %s. (%s s)' % (i + 1, coll, str(round(time.time() - timer_start_docs, 4))))
          timer_start_docs = time.time()
        if i+1 == nr_of_docs:
          logger.info('[EXTRACTOR] Successfully transferred collection %s (%d documents in %s seconds).' % (coll, i+1, str(round(time.time() - start, 4))))
        r.insert(doc)
        if r.has_pk is False and doc['_id']:
          r.add_pk('_id')

  def transfer_conf(self, coll_names):
    """
    Transfers documents or whole collections if the number of fields is less than 30 000 (batch_size).
    Types of attributes are determined using the collections.yml file.
    Returns
    -------
    -
    Parameters
    ----------
    coll_names : list
               : list of collection names
    """
    start_bulk = time.time()

    if collection.check(self.mdb, coll_names) is False:
      return

    if self.drop:
      table.drop(self.pg, self.schema_name, coll_names)
    elif self.truncate:
      table.truncate(self.pg, self.schema_name, coll_names)
    
    schema.create(self.pg, self.schema_name)

    for coll in coll_names:
      self.transfer_coll(coll)

  def transfer_coll(self, coll):
    '''
    Transfers documents or whole collections if the number of fields is less than 30 000 (batch_size).
    Returns
    -------
    -
    Parameters
    ----------
    coll : string
         : name of collection which is going to be transferred
    '''
    (attrs_new, attrs_original, types, relation_name, extra_props_type) = cp.config_fields(self.coll_settings, coll)
    if (attrs_new, attrs_original, types, relation_name, extra_props_type) == ([],[],[],[],[]):
      return
    r = relation.Relation(self.pg, self.schema_name, relation_name)
    extra_props_pg = "_extra_props"
    extra_props_mdb = "extraProps"
    attrs_mdb = attrs_original
    attrs_conf = attrs_new
    types_conf = types
    attrs_details = {}

    attrs_conf.append(extra_props_pg)
    types_conf.append(extra_props_type)
    attrs_mdb.append(extra_props_mdb)

    nr_of_attrs = len(attrs_mdb)

    for i in range(nr_of_attrs):
      details = {}
      details["name_conf"] = attrs_conf[i]
      details["type_conf"] = types[i]
      details["value"] = None
      attrs_details[attrs_mdb[i]] = details

    start = time.time()
    docs = collection.get_by_name(self.mdb, coll)

    timer_start_docs = start
    nr_of_docs = docs.count()
    transferring = []
    nr_of_transferred = 1000

    # TODO insert function call here
    r.columns_update(attrs_details)

    i = 0
    transferring = []
    for doc in docs:
      transferring.append(doc)  
      try:
        if (i+1)%nr_of_transferred==0 and i+1>=nr_of_transferred:
          r.insert_config_bulk(transferring, attrs_details)
          transferring = []
        if i + 1 == nr_of_docs and (i + 1) % nr_of_transferred != 0:
          if table.exists(self.pg, self.schema_name, relation_name):
            r.insert_config_bulk(transferring, attrs_details)
            logger.info('[EXTRACTOR] Successfully transferred collection %s (%d documents).' % (coll, i + 1))
            transferring = []
          else:
            logger.error('[EXTRACTOR] Table does %s might be deleted.' % relation_name)
            return
      except Exception as ex:
        logger.error('[EXTRACTOR] Transfer unsuccessful. %s' % ex)
      i += 1

  def transfer_doc(self, doc, r, coll):
    '''
    Transfers single document.
    Returns
    -------
    -
    Parameters
    ----------
    doc : dict
        : document 
    r : Relation
        relation in PG
    coll : string
         : collection name
    '''
    (attrs_new, attrs_original, types, relation_name, extra_props_type) = cp.config_fields(self.coll_settings, coll)
    if (attrs_new, attrs_original, types, relation_name, extra_props_type) == ([],[],[],[],[]):
      return
    # Adding _extra_props to inserted/updated row is necessary 
    # because this attribute is not part of the original document and anything
    # that is not defined in the collection.yml file will be pushed in this value.
    # This function will also create a dictionary which will contain all the information
    # about the attribute before and after the conversion.
    attrs_details = self.append_extra_props(attrs_new, attrs_original, types, extra_props_type)
    
    try:
      r.columns_update(attrs_details)
      r.insert_config_bulk([doc], attrs_details)
    except Exception as ex:
      logger.error('[EXTRACTOR] Transferring item was unsuccessful. %s' % ex)

  def append_extra_props(self, attrs_conf, attrs_mdb, types_conf, extra_props_type):
    '''
    Adds extra properties field.
    This field needs to be added like this because it is not part of the original document.
    It can also have any type.
    Returns
    -------
    attrs_details : list
                  : attribute details with extra property

    Parameters
    ----------
    attrs_conf : list
                : attribute names from config file
    attrs_mdb : list
                : field names of MongoDB document
    types_conf : list
                : types from config files
    extra_props_type : string
                : type of the extra property
    Example
    -------
    attrs_new = [kit_cat, birdy_bird]
    attrs_original = [kitCat, birdyBird]
    types = ['text', 'text']
    extra_props_type = 'jsonb'
    res = append_extra_props(attrs_new, attrs_original, types, extra_props_type)
    '''
    extra_props_pg = "_extra_props"
    extra_props_mdb = "extraProps"

    attrs_conf.append(extra_props_pg)
    attrs_mdb.append(extra_props_mdb)
    types_conf.append(extra_props_type)

    attrs_details = {}
    
    nr_of_attrs = len(attrs_mdb)

    for i in range(nr_of_attrs):
      details = {}
      details["name_conf"] = attrs_conf[i]
      details["type_conf"] = types_conf[i]
      details["value"] = None
      attrs_details[attrs_mdb[i]] = details
    return attrs_details