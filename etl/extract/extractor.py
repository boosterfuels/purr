import pymongo
import time
from extract import collection
from load import table, row
from transform import relation

from datetime import datetime, timedelta
from bson import Timestamp

class Extractor():
  """
  This is a class for extracting data from collections.
  """

  def __init__(self):
    """Constructor for Extractor"""

  def transfer(self, coll_names, truncate, drop):
    if collection.check(coll_names) is True:
      print('Transfering collections', coll_names)
    else:
      return

    fields = []

    for coll in coll_names:
      cols_and_types = []
      if table.exists(coll) is True:
        if truncate:
          table.truncate(coll_names)
        if drop:
          table.drop(coll_names)
          table.create(coll)
      else:
        table.create(coll)

      r = relation.Relation(coll)

      for doc in collection.get_by_name(coll):
        r.insert(doc)
        

