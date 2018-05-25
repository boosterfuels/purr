from extract import collection
from load import schema
from extract import tailer, extractor
import time
import sys
from datetime import datetime
from load import init_pg as postgres
from extract import init_mongo as mongodb

def transfer_collections(collections, truncate, drop, settings):
  """
  Parameters
  ----------
  collections
  truncate
  drop
  Example
  -------
  transfer_collections(['Feedback', 'Vehicle', 'Customer', 'Terminal'])
  TODO
  ----
  - create table with attributes and types
  """
  start_date_time = datetime.utcnow()  
  setup_pg = settings['postgres']
  setup_mdb = settings['mongo']
  pg = postgres.PgConnection(setup_pg['db_name'], setup_pg['user'])
  mongo = mongodb.MongoConnection(setup_mdb['db_name'])
  
  ex = extractor.Extractor()
  if setup_pg['table_drop'] is True:
    drop = True
  elif setup_pg['table_truncate'] is True:
    truncate = True

  if setup_pg['schema_reset'] is True:
    schema.reset(pg, setup_pg["schema_name"])
  ex.transfer_auto(collections, truncate, drop, pg, mongo.conn, setup_pg["schema_name"])
  if settings['tailing'] is True:
    start_tailing(start_date_time, pg, setup_pg["schema_name"])

def start_tailing(start_date_time, pg, schema):
  if start_date_time is None:
    start_date_time = datetime.utcnow()
  t = tailer.Tailer(pg, schema)
  t.start(start_date_time)

def get_collection_names():
  return collection.get_names()

def reset_schema(conn, schema_name):
  schema.reset(conn, schema_name)