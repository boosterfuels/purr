import pymongo
from load import table, row, constraint
from transform import type_checker, keyword_checker, config_parser, unnester
from bson.json_util import default

reserved = keyword_checker.get_keywords()

class Relation():
  """
  This is the main parents class for transforming data.
  """
  def __init__(self, pg, schema, collection_name):
    """Constructor for Relation"""
    self.relation_name = collection_name
    self.column_names = []
    self.column_types = []
    self.has_pk = False
    self.created = False
    self.db = pg
    self.schema = schema
    
  def exists(self):
    self.created = table.exists(self.db, self.schema, self.relation_name)
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
    row.insert(self.db, self.schema, self.relation_name, reduced_attributes, values)


  def insert_bulk(self, docs):
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
    attributes_all = []
    print("Transferring", len(docs), type(docs))
    
    if len(docs) != 0:
      attributes_all = list(docs[0].keys())

    for doc in docs:
      attributes = list(doc.keys())      
      diff = list(set(attributes) - set(attributes_all))
      attributes_all = attributes_all + diff 
    attributes_all = [attr.lower() for attr in attributes_all]

    result = []

    for doc in docs:
      attributes = list(doc.keys())
      (reduced_attributes, values) = self.get_attrs_and_vals(attributes, doc)
      diff = list(set(attributes_all) - set(reduced_attributes))
      # print(reduced_attributes)
      # print(attributes_all)
      # needed to keep the order
      for d in diff:
        reduced_attributes.append(d)
        values.append(None)
      dict_sorted = sorted(dict(zip(reduced_attributes, values)).items())
      values = (*[x[1] for x in dict_sorted],)

      result.append(values)
    attributes_all.sort()


      # diff = list(set(attributes) - set(attributes_all))
      # attributes_all = attributes_all + diff 
    # print(attributes_all)
    # table.add_multiple_columns(self.db, self.schema, self.relation_name, reduced_attributes, types)
    
    # for doc in docs:
    #   values = 
    # (reduced_attributes, values) = self.get_attrs_and_vals(attributes, doc)
    row.insert_bulk(self.db, self.schema, self.relation_name, attributes_all, result)

  def insert_config(self, doc, attrs, attrs_and_types):
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
    # print(type(doc))
    attrs_temp = [a.replace("_", "") for a in attrs]
    # print(attrs_temp)
    # print("DOC", doc)
    # print("ATTRS", attrs_and_types)
    values = [None] * len(attrs)
    for k, v in doc.items():
      k = k.lower().replace("_", "")
      if k in attrs_temp:
        idx = attrs_temp.index(k)
        value = unnester.cast_prim(attrs_and_types[k], v)
        values[idx] = value
      else:
        continue
    
    # print(attrs_temp)
    # print(values)
    row.insert(self.db, self.schema, self.relation_name, attrs, values)


  def insert_config_bulk(self, docs, attrs, attrs_and_types):
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
    attrs_temp = [a.replace("_", "") for a in attrs]
    result = []
    for doc in docs:
      _extra_props = {}
      values = [None] * len(attrs)
      for k, v in doc.items():
        k = k.lower().replace("_", "")
        if k in attrs_temp:
          idx = attrs_temp.index(k)
          value = unnester.cast_prim(attrs_and_types[k], v)
          values[idx] = value
        else:
          _extra_props.update({k: v})

      _extra_props = unnester.cast_prim('jsonb', _extra_props)
      if _extra_props != "{}":
        values[attrs.index("_extra_props")] = _extra_props
      result.append(tuple(values))
    row.insert_bulk(self.db, self.schema, self.relation_name, attrs, result)

  def update(self, doc):
    attributes = list(doc.keys())
    (reduced_attributes, values) = self.get_attrs_and_vals(attributes, doc)
    row.update(self.db, self.schema, self.relation_name, reduced_attributes, values)

  def delete(self, doc):
    attributes = list(doc.keys())
    row.delete(self.db, self.schema, self.relation_name, str(doc["_id"]))

  def get_attrs_and_vals(self, attributes, doc):
    """
    Gets all attributes and values needed to insert or update one raw
    """
    reduced_attributes = []
    values = []
    types = []
    col_names_types = table.get_column_names_and_types(self.db, self.schema, self.relation_name)
    
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

      value = unnester.cast(column_type, value)
      values.append(value)
      
      attr = attr.lower()

      if len(self.column_names) != 0:
        if attr not in self.column_names:
          table.add_column(self.db, self.schema, self.relation_name, attr, column_type)
        else:
          # Check if types are equal.
          idx_original = self.column_names.index(attr)
          type_orig = self.column_types[idx_original]
          type_new = column_type

          if type_orig != type_new:
            attr_new = type_checker.rename(attr, type_orig, type_new)
            if attr_new is not None:
              if attr_new not in self.column_names:
                table.add_column(self.db, self.schema, self.relation_name, attr_new, type_new)
                self.column_names.append(attr_new)
                self.column_types.append(type_new)
              attr = attr_new

      reduced_attributes.append(attr)
      if len(self.column_names) == 0:
        types.append(column_type)

    if len(self.column_names) == 0:
      # - get column names and their types
      table.add_multiple_columns(self.db, self.schema, self.relation_name, reduced_attributes, types)
    return reduced_attributes, values
  
  def create(self):
    table.create(self.db, self.schema, self.relation_name)

  def create_with_columns(self, attrs, types):
    table.create(self.db, self.schema, self.relation_name, attrs, types)

  def add_pk(self, attr):
    constraint.add_pk(self.db, self.schema, self.relation_name, attr)
    self.has_pk = True