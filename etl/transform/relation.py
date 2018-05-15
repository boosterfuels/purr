import pymongo
import time
from extract import collection
from load import table, row, constraint
from transform import typechecker, keywordchecker, unnest, config_parser
from datetime import datetime, timedelta
from bson import Timestamp

class Relation():
  """
  This is the main parents class for transforming data.
  """
  def __init__(self, collection_name):
    """Constructor for Relation"""
    self.relation_name = collection_name
    self.column_names = []
    self.column_types = []
    self.has_pk = False
    self.created = False

  def exists(self):
    self.created = table.exists(self.relation_name)
    return self.created

  def insert(self, doc):
    attributes = list(doc.keys())

    """
    Transforms document and inserts it into the corresponding table.
    Parameters
    ----------
    doc : dict
          the document we want to insert
    TODO 
    CHECK if self.column_names and self.column_types are still the same, do not
    """
    # This is needed because sometimes there is no value for attributes (null)
    # - in this case 
    (reduced_attributes, values) = self.get_attrs_and_vals(attributes, doc)
    row.insert(self.relation_name, reduced_attributes, values)

  def update(self, doc):
    attributes = list(doc.keys())
    (reduced_attributes, values) = self.get_attrs_and_vals(attributes, doc)
    row.update(self.relation_name, reduced_attributes, values)

  def delete(self, doc):
    attributes = list(doc.keys())
    row.delete(self.relation_name, doc["_id"])

  def get_attrs_and_vals(self, attributes, doc):
    """
    Gets all attributes and values needed to insert or update one raw
    """
    reduced_attributes = []
    values = []
    types = []
    col_names_types = table.get_column_names_and_types(self.relation_name)

    for attr_name, attr_type in col_names_types:
      if attr_name not in self.column_names:
        self.column_names.append(attr_name.lower())
        self.column_types.append(attr_type.lower())

    for attr in attributes:
      value = doc[attr]
      (value, column_type) = typechecker.get_pg_type(value)

      # Jump over nulls because there is no point to add a type 
      # until a value exists. We need a value to determine the type and
      # a default type would require change of schema. 

      if value == 'null' or column_type == None:
        continue

      if column_type == 'json[]' or column_type == 'jsonb':
        value = unnest.transform_composites(value)
        values.append(value)

      elif column_type == 'text[]':
        value = unnest.transform_primitive_list(value, column_type)
        values.append(value)

      elif column_type == 'float' and typechecker.is_nan(value) is False:
        values.append(str(value))
      else:
        values.append("'" + str(value) + "'")
      
      attr = attr.lower()

      if len(self.column_names) != 0:
        if attr not in self.column_names:
          if column_type != None:
            table.add_column(self.relation_name, attr, column_type)
        else:
          # Check if types are equal.
          idx_original = self.column_names.index(attr)
          type_orig = self.column_types[idx_original]
          type_new = column_type

          if type_orig != type_new:
            attr_new = typechecker.rename(attr, type_orig, type_new)
            if attr_new is not None:
              table.add_column(self.relation_name, attr_new, type_new)
              attr = attr_new

      reduced_attributes.append(attr)
      types.append(column_type)

    if len(self.column_names) == 0:
      # - get column names and their types
      
      table.add_multiple_columns(self.relation_name, reduced_attributes, types)     

    return reduced_attributes, values

  def bulk_insert(self, coll_data, attrs_conf, attrs_old):
    """
      TODO: insert multiple rows at the same time
      - schema change
      - fill in with nulls
      - do not request everything
    """
    print("bulk insert", coll_data.count()) 

    values = []
    for col in coll_data:
      nr_of_attrs = len(attrs_conf)
      print(nr_of_attrs)
      for i in range(0, nr_of_attrs):
        field = attrs_old[i]
        if field in col.keys():
          values.append("'" + str(col[field]) + "'")
          print(attrs_conf)
        else:
          values.append("'null'")

      row.insert(self.relation_name, attrs_conf, values)
      values = []
  
  def create(self):
    table.create(self.relation_name)

  def add_pk(self, attr):
    constraint.add_pk(self.relation_name, attr)
    self.has_pk = True