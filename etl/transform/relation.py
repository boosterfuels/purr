import pymongo
import time
from extract import collection
from load import table, row
from transform import typechecker, keywordchecker, unnest

from datetime import datetime, timedelta
from bson import Timestamp

class Relation():
  """
  This is the main parents class for transforming data.
  """
  def __init__(self, collection_name):
    """Constructor for Relation"""
    self.relation_name = collection_name.lower()
    self.column_names = []
    self.column_types = []

  def solve_diffs(self, cols_and_types, doc):
    fields = list(doc.keys())

    for col_n, col_t in cols_and_types:
      self.column_names.append(col_n)
      self.column_types.append(col_n)
    if set(fields) == set(self.column_names):
      print('Equal')    
    else:
      print('Not equal')

  def insert(self, doc):
    # solve_diffs && check_column_types()
    attributes = list(doc.keys())

    """
    Transforms document and inserts it into the corresponding table.
    Parameters
    doc : dict
          the document we want to insert
    ----------
    TODO 
    CHECK if self.column_names and self.column_types are still the same, do not
    """
    # This is needed because sometimes there is no value for attributes (null)
    # - in this case 
    (reduced_attributes, values) = self.get_attrs_and_vals(attributes, doc)
    row.insert(self.relation_name, reduced_attributes, values)

  def update(self, doc):
    attributes = list(doc.keys())
    print(doc)
    # This is needed because sometimes there is no value for attributes (null)
    # - in this case 
    (reduced_attributes, values) = self.get_attrs_and_vals(attributes, doc)
    row.update(self.relation_name, reduced_attributes, values)


  def get_attrs_and_vals(self, attributes, doc):
    reduced_attributes = []
    values = []
    for attr in attributes:
      value = doc[attr]
      (value, column_type) = typechecker.get_pg_type(value)

      if value == 'null' or column_type == None:
        continue

      if column_type == 'json[]' or column_type == 'jsonb':
        value = unnest.transform_composites(value)
        values.append(value)

      elif column_type == 'text[]':
        value = unnest.transform_primitive_list(value, column_type)
        values.append(value)

      elif column_type == 'float':
        #TODO check if the following line is necessary
        values.append(str(value))
      else:
        values.append("'" + str(value) + "'")

      if table.column_exists(self.relation_name, attr) == False and column_type != None:
        table.add_column(self.relation_name, attr, column_type)
      
      reduced_attributes.append(attr)
    return reduced_attributes, values

