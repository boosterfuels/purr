from etl.load import table, row, constraint
from etl.transform import type_checker
from etl.transform import unnester


def init_values(attrs):
    """
    Sets value for every column to None
    """
    for k, v in attrs.items():
        attrs[k]["value"] = None
    return attrs


def set_values(attr_details, doc, _extra_props=None):
    """
    Casts values for a whole document
    """
    for key, field_value in doc.items():
        keys_cm = list(attr_details.keys())
        if key in keys_cm:
            field_type = attr_details[key]["type_cm"]
            value = unnester.cast(
                field_value, field_type)
            if value == 'undefined' and _extra_props is not None:
                _extra_props.update({key: field_value})
            else:
                attr_details[key]["value"] = value
        else:
            if _extra_props is not None:
                _extra_props.update({key: field_value})
    if _extra_props is not None:
        return attr_details, _extra_props
    else:
        return attr_details


def prepare_row_for_insert(attrs, doc, include_extra_props=None):
    _extra_props = {}
    attrs = init_values(attrs)

    (attrs, _extra_props) = set_values(attrs, doc, _extra_props)

    if include_extra_props is True:
        _extra_props = unnester.cast(_extra_props, 'jsonb')
        attrs["extraProps"]["value"] = _extra_props

    attrs_pg = [v["name_cm"]
                for k, v in attrs.items() if k in doc.keys()]
    values = [v["value"] for k, v in attrs.items() if k in doc.keys()]
    if len(doc.keys()) > len(attrs_pg) and include_extra_props:
        attrs_pg.append('_extra_props')
        values.append(_extra_props)
    return attrs_pg, values


def is_schema_changed(attrs_pg, types_pg, attrs_cm, types_cm):
    equal_attrs = set(attrs_cm) == set(attrs_pg)
    equal_types = set(types_cm) == set(types_pg)
    if len(attrs_pg) and (equal_attrs and equal_types):
        return False
    return True


class Relation():
    """
    This is the main parents class for transforming data.
    """

    def __init__(self, pg, schema, relation, created=False):
        """Constructor for Relation"""
        self.relation_name = relation
        self.created = created
        self.db = pg
        self.schema = schema

    def exists(self):
        self.created = table.exists(self.db, self.schema, self.relation_name)
        return self.created

    def insert_bulk(self, docs, attrs,
                    include_extra_props=True):
        """
        Transforms document and inserts it into the corresponding table.
        Parameters
        ----------
        doc : dict
              the document we want to insert
        TODO add unset
        """
        # This is needed because
        # sometimes there is no value for attributes (null)
        result = []
        if type(docs) is not list:
            docs = [docs]
        for doc in docs:
            (attrs_pg, values) = prepare_row_for_insert(attrs, doc)
            result.append(tuple(values))

        if self.created is True:
            row.upsert_bulk(self.db, self.schema,
                            self.relation_name, attrs_pg, result)
        else:
            row.insert_bulk(self.db, self.schema,
                            self.relation_name, attrs_pg, result)

    def insert_bulk_no_extra_props(self, docs, attrs,
                                   include_extra_props=True):
        """
        Transforms document and inserts it into the corresponding table.
        Parameters
        ----------
        docs : dict
              the documents we want to insert
         unset: string[]
              list of fields to unset
        """
        # This is needed because
        # sometimes there is no value for attributes (null)
        result = []
        if type(docs) is not list:
            docs = [docs]
        for doc in docs:
            attrs = init_values(attrs)
            attrs = set_values(attrs, doc)

            if len(docs) > 1:
                attrs_pg = [v["name_cm"] for k, v in attrs.items()]
                values = [v["value"] for k, v in attrs.items()]
            else:
                attrs_pg = [v["name_cm"]
                            for k, v in attrs.items() if k in doc.keys()]
                values = [v["value"]
                          for k, v in attrs.items() if k in doc.keys()]
            result.append(tuple(values))

        if self.created is True or len(docs) == 1:
            row.upsert_bulk(self.db, self.schema,
                            self.relation_name, attrs_pg, result)
        else:
            row.insert_bulk(self.db, self.schema,
                            self.relation_name, attrs_pg, result)

    def insert_bulk_no_extra_props_tailed(self,
                                          docs,
                                          attrs,
                                          include_extra_props=True):
        """
        Transforms document and inserts it into the corresponding table.
        Parameters
        ----------
        docs : dict
              the documents we want to insert

        """
        # This is needed because
        # sometimes there is no value for attributes (null)
        result = []
        if type(docs) is not list:
            docs = [docs]
        grouped_values = {}

        for i in range(len(docs)):
            attrs_pg = []
            values = []
            doc = docs[i]
            attrs = init_values(attrs)
            (attrs) = set_values(
                attrs, doc)

            attrs_pg = tuple([v["name_cm"]
                              for k, v in attrs.items() if k in doc.keys()])
            values = [v["value"]
                      for k, v in attrs.items() if k in doc.keys()]

            if attrs_pg not in grouped_values.keys():
                grouped_values[attrs_pg] = []
            grouped_values[attrs_pg].append(values)

        for k, v in grouped_values.items():
            attrs_pg = list(k)
            row.upsert_bulk_tail(self.db, self.schema,
                                 self.relation_name, attrs_pg, v)

    def delete(self, docs):
        ids = []
        if type(docs) is list:
            for doc in docs:
                ids.append(str(doc["_id"]))
        else:
            ids.append(str(docs["_id"]))

        row.delete(self.db, self.schema, self.relation_name, ids)

    def create_with_columns(self, attrs, types):
        table.create(self.db, self.schema, self.relation_name, attrs, types)

    def add_pk(self, attr):
        constraint.add_pk(self.db, self.schema, self.relation_name, attr)

    def columns_remove(self, attrs_pg, types_pg, attrs_cm, types_cm):
        """
        Check if attributes in Postgres exist in the collection map.
        If not, remove the column.
        """
        temp_attrs_cm = []
        temp_types_cm = []

        for i in range(0, len(attrs_pg)):
            try:
                # check if attributes in PG are part of the collection map)
                idx = attrs_cm.index(attrs_pg[i])
            except ValueError:
                # remove extra columns from PG (because they are no longer
                # part of the collection map)
                table.remove_column(
                    self.db, self.schema, self.relation_name, attrs_pg[i])
                attrs_pg[i] = None
                types_pg[i] = None
                continue
            temp_attrs_cm.append(attrs_cm[idx])
            temp_types_cm.append(types_cm[idx])
            del attrs_cm[idx]
            del types_cm[idx]

        attrs_pg = [x for x in attrs_pg if x is not None]
        types_pg = [x for x in types_pg if x is not None]

        attrs_cm = temp_attrs_cm + (attrs_cm)
        types_cm = temp_types_cm + (types_cm)

        return attrs_pg, types_pg, attrs_cm, types_cm

    def columns_add(self, attrs_pg, types_pg, attrs_cm, types_cm):
        """
        Adds columns to Postgres OR updates column types.
        """
        type_convert_fail = []

        if set(attrs_cm).issubset(set(attrs_pg)):
            # update types
            for i in range(len(attrs_pg)):
                if attrs_pg[i] in attrs_cm:
                    # type from the db and type from the config file
                    type_old = types_pg[i].lower()
                    type_new = types_cm[i].lower()

                    name_ts_no_tz = 'timestamp without time zone'
                    name_ts = 'timestamp'
                    if type_old == name_ts_no_tz and type_new == name_ts:
                        continue
                    elif type_old != type_new:
                        # type was changed
                        if type_checker.is_convertable(type_old, type_new):
                            table.column_change_type(
                                self.db,
                                self.schema,
                                self.relation_name,
                                attrs_pg[i],
                                type_new)
                        else:
                            type_convert_fail.append(
                                (attrs_pg[i], type_old))
                            continue
            return type_convert_fail
        else:
            # add new columns
            diff = list(set(attrs_cm) - set(attrs_pg))

            # get type of new attributes
            attrs_to_add = []
            types_to_add = []
            for d in diff:
                attrs_to_add.append(d)
                idx = attrs_cm.index(d)
                types_to_add.append(types_cm[idx])
            table.add_multiple_columns(
                self.db,
                self.schema,
                self.relation_name,
                attrs_to_add,
                types_to_add
            )

    def update_schema(self, attrs_types_cm):
        """
        Checks if there were any changes in the schema and adds/changes
        attributes if needed.
        If relation is already created in the database then we need
        to get the existing attributes and types and compare them
        to our new attributes and types from the config file.

        - find what's different in attributes from pg and conf
        - check existing attribute names
        - check types
        - check check is one is castable to the other
        """

        attrs_cm = []
        types_cm = []
        attrs_pg = []
        types_pg = []
        column_info = table.get_column_names_and_types(
            self.db, self.schema, self.relation_name)
        if self.exists() is True and column_info is not None:
            # Every attribute from pg and conf has to have the same order.
            # We are sorting by pg column names.
            attrs_types_pg = dict(column_info)
            attrs_pg = [k for k in sorted(attrs_types_pg.keys())]
            types_pg = [attrs_types_pg[k]
                        for k in sorted(attrs_types_pg.keys())]

            attrs_cm = [attrs_types_cm[k]["name_cm"]
                        for k in attrs_types_cm.keys()]
            types_cm = [attrs_types_cm[k]["type_cm"]
                        for k in attrs_types_cm.keys()]

            # if attributes from PG and the collection map are
            # the same, do nothing
            schema_changed = is_schema_changed(
                attrs_pg, types_pg, attrs_cm, types_cm)

            if schema_changed is False:
                return

            (attrs_pg, types_pg, attrs_cm, types_cm) = self.columns_remove(
                attrs_pg, types_pg, attrs_cm, types_cm)

            self.columns_add(attrs_pg, types_pg, attrs_cm, types_cm)
        else:
            attrs_cm = [v["name_cm"] for k, v in attrs_types_cm.items()]
            types_cm = [v["type_cm"] for k, v in attrs_types_cm.items()]
            if self.exists() is False:
                self.create_with_columns(attrs_cm, types_cm)
            return
        # TODO if table was dropped or schema was reset
        # then there is no need to have fun
        # with the type checking.
        # if len(attrs_types_from_db) == 0:

        # When new attributes are fully contained in the attribute list from DB
        # we need to check if the types are equal and if not,
        # we need to check if it is possible to convert the
        # old type into the new one.
        # Anything can be converted to JSONB.

    def vacuum(self):
        table.vacuum(self.db, self.schema, self.relation_name)
