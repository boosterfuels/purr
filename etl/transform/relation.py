import pymongo

from etl.load import table, row, constraint
from etl.transform import type_checker, keyword_checker, config_parser, unnester

reserved = keyword_checker.get_keywords()

class Relation():
  """
  This is the main parents class for transforming data.
  """
  def __init__(self, pg, schema, relation, created=False):
    """Constructor for Relation"""
    self.relation_name = relation
    self.column_names = []
    self.column_types = []
    self.has_pk = False
    self.created = created
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
    row.insert_bulk(self.db, self.schema, self.relation_name, attributes_all, result, unset)

  def insert_config_bulk(self, docs, attrs, include_extra_props = True, unset = []):
    """
    Transforms document and inserts it into the corresponding table.
    Parameters
    ----------
    doc : dict
          the document we want to insert
    TODO add unset
    CHECK if self.column_names and self.column_types are still the same, do not
    """
    # This is needed because sometimes there is no value for attributes (null)
    result = []
    if type(docs) is not list:
      docs = [docs]
    for doc in docs:
      _extra_props = {}
      for k, v in attrs.items():
        attrs[k]["value"] = None
      for key_doc, value_doc in doc.items():
        keys_conf = list(attrs.keys())
        if key_doc in keys_conf:
          value = unnester.cast(attrs[key_doc]["type_conf"], value_doc)
          if value == 'undefined':
            _extra_props.update({key_doc: value_doc})          
          else:
            attrs[key_doc]["value"] = value
        else:
          _extra_props.update({key_doc: value_doc})
      if include_extra_props is True:
        _extra_props = unnester.cast('jsonb', _extra_props)
        attrs["extraProps"]["value"] = _extra_props

      if len(docs) > 1:
        attrs_pg = [v["name_conf"] for k, v in attrs.items()]
        values = [v["value"] for k, v in attrs.items()]
      else:
        attrs_pg = [v["name_conf"] for k, v in attrs.items() if k in doc.keys()]
        values = [v["value"] for k, v in attrs.items() if k in doc.keys()]
        if len(doc.keys()) > len(attrs_pg) and include_extra_props is True:
          attrs_pg.append('_extra_props')
          values.append(_extra_props)

      result.append(tuple(values))

    if self.created is True:
      row.upsert_bulk(self.db, self.schema, self.relation_name, attrs_pg, result)
    else:
      row.insert_bulk(self.db, self.schema, self.relation_name, attrs_pg, result)



  def insert_config_bulk_no_extra_props(self, docs, attrs, include_extra_props = True, unset = []):
    """
    Transforms document and inserts it into the corresponding table.
    Parameters
    ----------
    docs : dict
          the documents we want to insert
     unset: string[]
          list of fields to unset
    TODO 
    CHECK if self.column_names and self.column_types are still the same
    """
    # This is needed because sometimes there is no value for attributes (null)
    result = []
    if type(docs) is not list:
      docs = [docs]
    for doc in docs:
      for k, v in attrs.items():
        attrs[k]["value"] = None
      for key_doc, value_doc in doc.items():
        keys_conf = list(attrs.keys())
        if key_doc in keys_conf:
          value = unnester.cast(attrs[key_doc]["type_conf"], value_doc)
          if value != 'undefined':
            attrs[key_doc]["value"] = value

      if len(docs) > 1:
        attrs_pg = [v["name_conf"] for k, v in attrs.items()]
        values = [v["value"] for k, v in attrs.items()]
      else:
        attrs_pg = [v["name_conf"] for k, v in attrs.items() if k in doc.keys()]
        values = [v["value"] for k, v in attrs.items() if k in doc.keys()]
        for u in unset:
          attrs_pg.append(attrs[u]["name_conf"])
          values.append(None)
      result.append(tuple(values))

    if self.created is True or len(docs) == 1:
      row.upsert_bulk(self.db, self.schema, self.relation_name, attrs_pg, result)
    else:
      row.insert_bulk(self.db, self.schema, self.relation_name, attrs_pg, result)


  def insert_config_bulk_no_extra_props_tailed(self, docs, attrs, include_extra_props = True, unset = []):
    """
    Transforms document and inserts it into the corresponding table.
    Parameters
    ----------
    docs : dict
          the documents we want to insert
     unset: string[]
          list of fields to unset
    TODO 
    CHECK if self.column_names and self.column_types are still the same
    """
    # This is needed because sometimes there is no value for attributes (null)
    result = []
    if type(docs) is not list:
      docs = [docs]
    for doc in docs:
      for k, v in attrs.items():
        attrs[k]["value"] = None
      for key_doc, value_doc in doc.items():
        keys_conf = list(attrs.keys())
        if key_doc in keys_conf:
          value = unnester.cast(attrs[key_doc]["type_conf"], value_doc)
          if value != 'undefined':
            attrs[key_doc]["value"] = value

      if len(docs) > 1:
        attrs_pg = [v["name_conf"] for k, v in attrs.items()]
        values = [v["value"] for k, v in attrs.items()]
      else:
        attrs_pg = [v["name_conf"] for k, v in attrs.items() if k in doc.keys()]
        values = [v["value"] for k, v in attrs.items() if k in doc.keys()]
        for u in unset:
          attrs_pg.append(attrs[u]["name_conf"])
          values.append(None)
      result.append(tuple(values))

    if self.created is True or len(docs) == 1:
      row.upsert_bulk_tail(self.db, self.schema, self.relation_name, attrs_pg, result)
    else:
      row.insert_bulk(self.db, self.schema, self.relation_name, attrs_pg, result)

  def update(self, doc):
    attributes = list(doc.keys())
    (reduced_attributes, values) = self.get_attrs_and_vals(attributes, doc)
    row.update(self.db, self.schema, self.relation_name, reduced_attributes, values)

  def delete(self, docs):
    ids = []
    if type(docs) is list:
      for doc in docs:
        ids.append(str(doc["_id"]))
    else:
      ids.append(str(docs["_id"]))

    row.delete(self.db, self.schema, self.relation_name, ids)

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

  def udpate_types(self, attrs_types_conf):
      """
      Checks if there were any changes in the schema and adds/changes attributes if needed. 
      If relation is already created in the database then we need to get the 
      existing attributes and types and compare them to our new attributes and types
      from the config file.
      
      - find what's different in attributes from pg and conf
      - check existing attribute names
      - check types
      - check check is one is castable to the other
      """
      
      attrs_conf = []
      types_conf = []
      attrs_pg = []
      types_pg = []
      column_info = table.get_column_names_and_types(self.db, self.schema, self.relation_name)
      if self.exists() is True and column_info is not None:
        # Every attribute from pg and conf has to have the same order. We are sorting by pg column names.
        attrs_types_pg = dict(column_info)
        attrs_pg = [k for k in sorted(attrs_types_pg.keys())]
        types_pg = [attrs_types_pg[k] for k in sorted(attrs_types_pg.keys())]

        attrs_conf = [attrs_types_conf[k]["name_conf"] for k in attrs_types_conf.keys()]
        types_conf = [attrs_types_conf[k]["type_conf"] for k in attrs_types_conf.keys()]

        # if attributes from PG and the collection map are the same, do nothing
        if(len(attrs_pg) != 0 and (set(attrs_conf) == set(attrs_pg)) and set(types_conf) == set(types_pg)):
          return

        temp_attrs_conf = []
        temp_types_conf = []

        for i in range(0, len(attrs_pg)):
          try:
            # check if attributes in PG are part of the collection map)
            idx = attrs_conf.index(attrs_pg[i])
          except ValueError:
            table.remove_column(self.db, self.schema, self.relation_name, attrs_pg[i])
            attrs_pg[i] = None
            types_pg[i] = None
            continue
          temp_attrs_conf.append(attrs_conf[idx])
          temp_types_conf.append(types_conf[idx])
          del attrs_conf[idx]
          del types_conf[idx]

        # remove extra columns from PG (because they are no longer part of the collection map)
        attrs_pg = [x for x in attrs_pg if x is not None]
        types_pg = [x for x in types_pg if x is not None]

        attrs_conf = temp_attrs_conf + (attrs_conf)
        types_conf = temp_types_conf + (types_conf)

        type_convert_fail = []

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
                  table.column_change_type(self.db, self.schema, self.relation_name, attrs_pg[i], type_new)
                else:
                  type_convert_fail.append((attrs_pg[i], type_old))
                  continue
          return type_convert_fail
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
        if self.exists() is False:
          self.create_with_columns(attrs_conf, types_conf)
        return
      # TODO if table was dropped or schema was reset then there is no need to have fun
      # with the type checking.
      # if len(attrs_types_from_db) == 0:
      
      # When new attributes are fully contained in the attribute list from DB
      # we need to check if the types are equal and if not, we need to check if
      # it is possible to convert the old type into the new one. 
      # Anything can be converted to JSONB.

  def is_convertable(self, type_old, type_new):
    """
    Returns True if type old can be converted to type new
    
    convertables: list of tuples
                : contains convertable types (type_old, type_new) 
    """ 
    convertables = [
      ('boolean', 'jsonb'),
      ('double precision', 'jsonb'),
      ('text', 'jsonb'),
      ('timestamp', 'jsonb'),
      ('jsonb', 'jsonb'),

      ('boolean', 'text'),
      ('double precision', 'text'),
      ('text', 'text'),
      ('timestamp', 'text'),
      ('jsonb', 'text'),
    ]
    if (type_old, type_new) in convertables:
      return True
    return False
