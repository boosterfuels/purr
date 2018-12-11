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

name_extra_props_pg = "_extra_props"
name_extra_props_mdb = "extraProps"

class Extractor():
  """
  This is a class for extracting data from collections.
  """

  def __init__(self, pg, mdb, settings_pg, settings_general, coll_settings):
    """Constructor for Extractor"""

    self.pg = pg
    self.mdb = mdb
    self.typecheck_auto = settings_general['typecheck_auto']
    self.include_extra_props = settings_general['include_extra_props']
    try: 
      self.include_extra_props = settings_general['include_extra_props']
    except KeyError:
      self.include_extra_props = False
    self.schema_name = settings_pg["schema_name"]
    self.truncate = settings_pg['table_truncate']
    self.drop = settings_pg['table_drop']
    self.coll_settings = coll_settings
    self.tailing_from = settings_general['tailing_from']
    self.tailing_from_db = settings_general['tailing_from_db']
    
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
      r = relation.Relation(self.pg, self.schema_name, coll)
      table.create(self.pg, self.schema_name, coll)
      docs = collection.get_by_name(self.mdb, coll)
      nr_of_docs = docs.count()
      for i in range(nr_of_docs):
        doc = docs[i]
        if (i+1)%10000==0 and i+1>=10000:
          logger.info('[EXTRACTOR] Transferred %d documents from collection %s. (%s s)' % (i + 1, coll))
        if i+1 == nr_of_docs:
          logger.info('[EXTRACTOR] Successfully transferred collection %s (%d documents).' % (coll, i+1))
        r.insert(doc)
        if r.has_pk is False and doc['_id']:
          r.add_pk('_id')

  def transfer_conf(self, coll_names_in_config):
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

    coll_names = collection.check(self.mdb, coll_names_in_config)
    if len(coll_names) == 0:
      logger.info('[EXTRACTOR] No collections.')
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
    
    (r, attrs_details) = self.adjust_columns(coll)
    
    if self.tailing_from is not None or self.tailing_from_db is True:
      return

    if self.include_extra_props is True:
      docs = collection.get_by_name(self.mdb, coll)
    else:
      attr_source = [k for k, v in attrs_details.items()]
      docs = collection.get_by_name_reduced(self.mdb, coll, attr_source)

    # Start transferring docs
    nr_of_docs = docs.count()
    nr_of_transferred = 1000
    i = 0
    transferring = []
    for doc in docs:
      transferring.append(doc)  
      try:
        if (i+1)%nr_of_transferred==0 and i+1>=nr_of_transferred:
          if self.include_extra_props is True:
            r.insert_config_bulk(transferring, attrs_details, self.include_extra_props)
          else:
            print("nr_of_tr", i+1)
            r.insert_config_bulk_no_extra_props(transferring, attrs_details, self.include_extra_props)
          transferring = []
        if i + 1 == nr_of_docs and (i + 1) % nr_of_transferred != 0:
          if self.include_extra_props is True:
            r.insert_config_bulk(transferring, attrs_details, self.include_extra_props)
          else:
            print("nr_of_tr", i+1)
            r.insert_config_bulk_no_extra_props(transferring, attrs_details, self.include_extra_props)
            logger.info('[EXTRACTOR] Successfully transferred collection %s (%d documents).' % (coll, i + 1))
            transferring = []
      except Exception as ex:
        logger.error('[EXTRACTOR] Transfer unsuccessful. %s' % ex)
      i += 1

  def transfer_doc(self, doc, r, coll):
    '''
    Transfers single document.
    Parameters
    ----------
    doc : dict
        : document 
    r : Relation
        relation in PG
    coll : string
         : collection name
    Returns
    -------
    -

    Raises
    ------
    Example
    -------
    '''
    (attrs_new, attrs_original, types, relation_name, type_extra_props_pg) = cp.config_fields(self.coll_settings, coll)
    if (attrs_new, attrs_original, types, relation_name, type_extra_props_pg) == ([],[],[],[],[]):
      return
    # Adding extra properties to inserted/updated row is necessary 
    # because this attribute is not part of the original document and anything
    # that is not defined in the collection.yml file will be pushed in this value.
    # This function will also create a dictionary which will contain all the information
    # about the attribute before and after the conversion.

    attrs_details = self.prepare_attr_details(attrs_new, attrs_original, types, type_extra_props_pg)
    try:
      r.udpate_types(attrs_details)
      if self.include_extra_props is True:
        r.insert_config_bulk([doc], attrs_details, self.include_extra_props)
      else:
        r.insert_config_bulk_no_extra_props([doc], attrs_details, self.include_extra_props)
    except Exception as ex:
      logger.error('[EXTRACTOR] Transferring to %s was unsuccessful. Exception: %s' % (r.relation_name, ex))
      logger.error('%s\n', ([doc]))

  def prepare_attr_details(self, attrs_conf, attrs_mdb, types_conf, type_extra_props_pg = None):
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
    if self.include_extra_props is True:


      attrs_conf.append(name_extra_props_pg)
      attrs_mdb.append(name_extra_props_mdb)
      types_conf.append(type_extra_props_pg)

    attrs_details = {}
    for i in range(0, len(attrs_mdb)):
      details = {}
      details["name_conf"] = attrs_conf[i]
      details["type_conf"] = types_conf[i]
      details["value"] = None
      attrs_details[attrs_mdb[i]] = details
    return attrs_details

  def adjust_columns(self, coll):
    """
    Adds or removes extra properties if necessary and updates column types.
    """
    # get data from collection map
    (attrs_new, attrs_original, types, relation_name, type_extra_props) = cp.config_fields(self.coll_settings, coll)
    if (attrs_new, attrs_original, types, relation_name, type_extra_props) == ([],[],[],[],[]):
      return
    
    r = relation.Relation(self.pg, self.schema_name, relation_name)
    
    # This dict contains all the necessary information about the Mongo fields, Postgres columns and their types
    attrs_details = {}
    attrs_mdb = attrs_original
    attrs_conf = attrs_new
    types_conf = types

    if self.include_extra_props is True:
      attrs_conf.append(name_extra_props_pg)
      types_conf.append(type_extra_props)
      attrs_mdb.append(name_extra_props_mdb)
      
      # Add column extra_props to table if it does not exist
      # table.add_column(self.pg, self.schema_name, r.relation_name, name_extra_props_pg, type_extra_props)
    # else:
    #   # Remove column extra_props from table if it exists
    #   table.remove_column(self.pg, r.relation_name, name_extra_props_pg)
    
    for i in range(len(attrs_mdb)):
      details = {}
      details["name_conf"] = attrs_conf[i]
      details["type_conf"] = types[i]
      details["value"] = None
      attrs_details[attrs_mdb[i]] = details

    # TODO insert function call here
    # Check if changing type was unsuccessful.
    type_update_failed = r.udpate_types(attrs_details)

    if type_update_failed is not None:
      for tuf in type_update_failed:
        name_pg = tuf[0]
        name_mdb = [attr for attr in attrs_details if attrs_details[attr]["name_conf"]==name_pg][0]
        type_orig = tuf[1].lower()
        type_new = attrs_details[name_mdb]["type_conf"].lower()
        attrs_details[name_mdb]["type_conf"] = type_orig
        logger.warn("[EXTRACTOR] Type conversion failed for column '%s'. Skipping conversion %s -> %s." % (name_pg, type_orig, type_new))

    return r, attrs_details