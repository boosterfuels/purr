from extract import collection
from load import pg_init as pg, table, row, schema
from extract import tailer, extractor
import time
import sys
from datetime import datetime

# check if db exists
# check if db is empty
# soft reload: keep current tables if not empty
# hard reload: drop db and restart

def transfer_collections(collections, truncate, drop):
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
  # collections = ['Audience', 'PaymentMethod', 'FuelRequest']
  ex = extractor.Extractor()
  ex.transfer_auto(collections, truncate, drop)

def start_tailing(start_date_time):
  if start_date_time is None:
    start_date_time = datetime.utcnow()
  t = tailer.Tailer()
  t.start(start_date_time)

def get_collection_names():
  return collection.get_names()

def reset_schema():
  schema.reset()