import pymongo
from bson.json_util import default

from etl.load import table, row, constraint
from etl.transform import type_checker, keyword_checker, config_parser, unnester

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
      # needed to keep the order
      for d in diff:
        reduced_attributes.append(d)
        values.append(None)
      dict_sorted = sorted(dict(zip(reduced_attributes, values)).items())
      values = (*[x[1] for x in dict_sorted],)

      result.append(values)
    attributes_all.sort()
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
    attrs_temp = [a.replace("_", "") for a in attrs]
    values = [None] * len(attrs)
    for k, v in doc.items():
      k = k.lower().replace("_", "")
      if k in attrs_temp:
        idx = attrs_temp.index(k)
        value = unnester.cast_prim(attrs_and_types[k], v)
        values[idx] = value
      else:
        continue
    
    row.insert(self.db, self.schema, self.relation_name, attrs, values)


  def insert_config_bulk(self, docs, attrs):
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

    result = []
    for doc in docs:
      _extra_props = {}
      values = [None] * len(attrs)
      for key_doc, value_doc in doc.items():
        keys_conf = list(attrs.keys())
        if key_doc in keys_conf:
          value = unnester.cast_prim(attrs[key_doc]["type_conf"], value_doc)
          if value == 'undefined':
            _extra_props.update({key_doc: value_doc})          
          else:
            attrs[key_doc]["value"] = value
        else:
          _extra_props.update({key_doc: value_doc})

      _extra_props = unnester.cast_prim('jsonb', _extra_props)
      attrs["extraProps"]["value"] = _extra_props
      values = [v["value"] for k, v in attrs.items()]
      attrs_pg = [v["name_conf"] for k, v in attrs.items()]
      result.append(tuple(values))
    row.insert_bulk(self.db, self.schema, self.relation_name, attrs_pg, result)

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

  def columns_update(self, attrs_types_conf):
      # check if there were any changes in the schema
      # If relation is already created in the database then we need to get the 
      # existing attributes and types and compare them to our new attributes and types
      # from the config file.
      attrs_conf = []
      types_conf = []
      attrs_pg = []
      types_pg = []
      if self.exists() is True:
        attrs_types_pg = dict(table.get_column_names_and_types(self.db, self.schema, self.relation_name))
        attrs_pg = [k for k,v in attrs_types_pg.items()]
        types_pg = [v for k, v in attrs_types_pg.items()]

        attrs_conf = [v["name_conf"] for k,v in attrs_types_conf.items()]
        types_conf = [v["type_conf"] for k, v in attrs_types_conf.items()]
        
        # - find what's different in attributes from pg and conf
        # - check existing attribute names
        # - check types
        # - check check is one is castable to the other
        if(len(attrs_pg) != 0 and (set(attrs_conf) == set(attrs_pg))):
          return
        if set(attrs_conf).issubset(set(attrs_pg)):
          # check types
          for i in range(len(attrs_pg)):
            if attrs_pg[i] in attrs_conf:
              # type from the db and type from the config file 
              type_old = types_pg[i].lower()
              type_new = types_conf[i].lower()
              if type_old == 'timestamp without time zone' and type_new == 'timestamp':
                continue
              elif type_old != type_new:
                if self.is_convertable(type_old, type_new):
                  table.column_change_type(pg, schema_name, relation_name, attr_db, type_new)
        else:
          # check old attrs and new ones
          diff = list(set(attrs_conf) - set(attrs_pg))

          # get type of new attributes
          attrs_to_add = []
          types_to_add = []
          for d in diff:
            attrs_to_add.append(d)
            idx = attrs_conf.index(d)
            types_to_add.append(types_conf[idx])
          table.add_multiple_columns(self.db, self.schema, self.relation_name, attrs_to_add, types_to_add)
        
      else:
        attrs_conf = [v["name_conf"] for k,v in attrs_types_conf.items()]
        types_conf = [v["type_conf"] for k,v in attrs_types_conf.items()]
        self.create_with_columns(attrs_conf, types_conf)
        return
      # TODO if table was dropped or schema was reset then there is no need to have fun
      # with the type checking.
      # if len(attrs_types_from_db) == 0:
      
      # When new attributes are fully contained in the attribute list from DB
      # we need to check if the types are equal and if not, we need to check if
      # it is possible to convert the old type into the new one. 
      # Anything can be converted to JSONB.

  def is_convertable(type_old, type_new):
    if type_new == 'jsonb':
      return True
    return False
