import pymongo
import time
from extract import collection
from load import table, row, constraint
from transform import type_checker, keyword_checker, config_parser, unnester
from datetime import datetime, timedelta
from bson import Timestamp
import json
from bson.json_util import default, ObjectId, dumps, loads, RELAXED_JSON_OPTIONS, CANONICAL_JSON_OPTIONS
import psycopg2.extras

reserved = keyword_checker.get_keywords()

class Relation():
  """
  This is the main parents class for transforming data.
  """
  def __init__(self, pg_conn, schema, collection_name):
    """Constructor for Relation"""
    self.relation_name = collection_name
    self.column_names = []
    self.column_types = []
    self.has_pk = False
    self.created = False
    self.conn = pg_conn
    self.schema = schema
    
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
    row.insert(self.conn, self.schema, self.relation_name, reduced_attributes, values)

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
    col_names_types = table.get_column_names_and_types(self.conn, self.schema, self.relation_name)
    
    for attr_name, attr_type in col_names_types:
      if attr_name not in self.column_names:
        self.column_names.append(attr_name.lower())
        self.column_types.append(attr_type.lower())

    for attr in attributes:
      # Add an underscore to the attribute if it is a reserved word in PG.
      if attr in reserved:
        attr = '_' + attr

      (value, column_type) = type_checker.get_pg_type(doc[attr])

      # Jump over nulls because there is no point to add a type 
      # until a value exists. We need a value to determine the type and
      # a default type would require change of schema. 
    
      if value == 'null' or column_type == None:
        continue

      if type(value) is ObjectId:
        values.append(str(value))

      elif column_type == 'jsonb[]':
        temp = []
        for v in value:
          temp.append(json.dumps(v, default=default))
        values.append(temp)

      elif column_type == 'jsonb':
        value = unnester.change_object_id(value)
        values.append(json.dumps(value, default=default))

      elif column_type == 'text[]':
        value = [str(v) for v in value]
        values.append(value)
        
      elif column_type == 'float' and type_checker.is_nan(value) is False:
        values.append(value)
      else:
        values.append(str(value))
      
      attr = attr.lower()

      if len(self.column_names) != 0:
        if attr not in self.column_names:
          if column_type != None:
            table.add_column(self.conn, self.schema, self.relation_name, attr, column_type)
        else:
          # Check if types are equal.
          idx_original = self.column_names.index(attr)
          type_orig = self.column_types[idx_original]
          type_new = column_type

          if type_orig != type_new:
            attr_new = type_checker.rename(attr, type_orig, type_new)
            if attr_new is not None:
              if attr_new not in self.column_names:
                table.add_column(self.conn, self.schema, self.relation_name, attr_new, type_new)
                self.column_names.append(attr_new)
                self.column_types.append(type_new)
              attr = attr_new

      reduced_attributes.append(attr)
      types.append(column_type)

    if len(self.column_names) == 0:
      # - get column names and their types
      
      table.add_multiple_columns(self.conn, self.schema, self.relation_name, reduced_attributes, types)     

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
      for i in range(0, nr_of_attrs):
        field = attrs_old[i]
        if field in col.keys():
          values.append("'" + str(col[field]) + "'")
        else:
          values.append("'null'")

      row.insert(self.relation_name, attrs_conf, values)
      values = []
  
  def create(self):
    table.create(self.conn, self.schema, self.relation_name)

  def add_pk(self, attr):
    constraint.add_pk(self.conn, self.schema, self.relation_name, attr)
    self.has_pk = True