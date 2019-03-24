import pymongo
import time

from etl.extract import collection, transfer_info
from etl.extract import collection_map as cm
from etl.load import table, row, constraint, schema
from etl.monitor import logger

from etl.transform import relation, type_checker as tc, config_parser as cp

from datetime import datetime, timedelta
from bson import Timestamp
import json

name_extra_props_pg = "_extra_props"
name_extra_props_mdb = "extraProps"


class Extractor():
    """
    This is a class for extracting data from collections.
    """

    def __init__(self, pg, mdb, settings_pg, settings_general, coll_def):
        """Constructor for Extractor"""

        self.pg = pg
        self.mdb = mdb
        self.include_extra_props = settings_general['include_extra_props']
        try:
            self.include_extra_props = settings_general['include_extra_props']
        except KeyError:
            self.include_extra_props = False
        self.schema = settings_pg["schema_name"]
        self.truncate = settings_pg['table_truncate']
        self.drop = settings_pg['table_drop']
        self.coll_def = coll_def
        self.tailing_from = settings_general['tailing_from']
        self.tailing_from_db = settings_general['tailing_from_db']
        self.coll_map_cur = cm.get_table(self.pg)
        self.attrs_details = {}

    def update_table_def(self, coll_map_cur, coll_map_new):
        for i in range(0, len(coll_map_cur)):
            name_coll = coll_map_cur[i][1]
            name_table = coll_map_cur[i][2]
            fields_cur = coll_map_cur[i][3]
            fields_new = coll_map_new[i][3]

            removed = [x for x in fields_cur if x not in fields_new]
            added = [x for x in fields_new if x not in fields_cur]

            sources_removed = [x[":source"] for x in removed]
            sources_added = [x[":source"] for x in added]

            source_persistent = [
                x for x in sources_removed if x in sources_added]

            if len(source_persistent):
                # (1) Try to convert the column
                # (2) If (1) was not successful (PG could not
                # convert the column), just rename it and add
                # the column again so Purr can take care of it
                for i in range(0, len(fields_new)):
                    field = fields_new[i]
                    if field[":source"] in source_persistent:
                        for column, v in field.items():
                            if v is None:
                                type_old = fields_cur[i][":type"]
                                type_new = field[":type"]
                                if tc.is_convertable(type_old, type_new):
                                    logger.info(
                                        """[EXTRACTOR] table %s, column %s:
                                        Type [%s] is convertable to [%s]""" % (
                                            name_table,
                                            column,
                                            type_old,
                                            type_new
                                        ))
                                    table.column_change_type(
                                        self.pg,
                                        self.schema,
                                        name_table,
                                        column,
                                        type_new)
                                else:

                                    logger.error("""
                                        [EXTRACTOR] In table %s, column %s:
                                        Type [%s] is NOT convertable to [%s]
                                        """ % (
                                        name_table,
                                        column,
                                        type_old,
                                        type_new))

            for item in added:
                if item[":source"] not in source_persistent:
                    for attribute, v in item.items():
                        if v is None:
                            self.coll_def[name_coll][":columns"] = fields_new
                            table.add_column(
                                self.pg,
                                self.schema,
                                name_table,
                                attribute,
                                item[":type"])
                            self.transfer_coll(coll_map_cur[i][1])

            for item in removed:
                if item[":source"] not in source_persistent:
                    for attribute, v in item.items():
                        if v is None:
                            # update collection settings
                            self.coll_def[name_coll][":columns"] = fields_new
                            table.remove_column(
                                self.pg,
                                self.schema,
                                name_table,
                                attribute)

    def table_track(self, coll_map_cur, coll_map_new):
        logger.info("[EXTRACTOR] Adding new collection")
        colls_cur = [x[1] for x in coll_map_cur]
        colls_new = [x[1] for x in coll_map_new]
        colls_to_add = [x for x in colls_new if x not in colls_cur]

        for name_coll in colls_to_add:
            coll_def = [x for x in coll_map_new if x[1] == name_coll][0]
            columns = coll_def[3]
            name_rel = coll_def[2]
            type_extra_prop = 'JSONB'

            meta = {
                ':table': name_rel,
                ':extra_props': type_extra_prop
            }

            self.coll_def[name_coll] = {
                ':columns': columns,
                ':meta': meta,
            }
            self.transfer_coll(name_coll)

    def table_untrack(self, coll_map_cur, coll_map_new):
        tables_cur = [x[2] for x in coll_map_cur]
        tables_remaining = [x[2] for x in coll_map_new]

        tables_to_drop = [x for x in tables_cur if x not in tables_remaining]
        colls_to_remove = [x[1]
                           for x in coll_map_cur if x[2] in tables_to_drop]

        logger.info("[EXTRACTOR] Stop syncing collections %s." %
                    ", ".join(colls_to_remove))

        for coll in colls_to_remove:
            self.coll_def.pop(coll, None)
        # TODO: what to do? drop or just stop tracking?
        # table.drop(self.pg, self.schema, tables_to_drop)

    def update_coll_map(self):
        # TODO: get types from pg
        logger.info("[EXTRACTOR] Updating schema from purr_collection_map")

        coll_map_cur = self.coll_map_cur
        coll_map_new = cm.get_table(self.pg)

        if coll_map_cur == coll_map_new:
            logger.info("[EXTRACTOR] Schema is not changed")
            return

        # If no tables were added or removed then update
        # table definition, otherwise add or remove a table.
        if len(coll_map_new) == len(coll_map_cur):
            self.update_table_def(coll_map_cur, coll_map_new)
        elif len(coll_map_new) > len(coll_map_cur):
            self.table_track(coll_map_cur, coll_map_new)
        else:
            self.table_untrack(coll_map_cur, coll_map_new)

        # update current collection map - from the db
        self.coll_map_cur = coll_map_new

        # TODO:
        # checking if the relation name is changed
        # restart transfer
        # update schema

    def transfer(self, coll_names_in_config):
        """
        Transfers documents or whole collections if the number of fields
        is less than 30 000 (batch_size).
        Types of attributes are determined using the collections.yml file.
        Returns
        -------
        -
        Parameters
        ----------
        coll_names : list
                   : list of collection names
        """

        coll_names = collection.check(self.mdb, coll_names_in_config)
        if len(coll_names) == 0:
            logger.info('[EXTRACTOR] No collections.')
            return

        if self.drop:
            table.drop(self.pg, self.schema, coll_names)
        elif self.truncate:
            table.truncate(self.pg, self.schema, coll_names)

        schema.create(self.pg, self.schema)

        for coll in coll_names:
            self.transfer_coll(coll)

    def transfer_coll(self, coll):
        '''
        Transfers documents or whole collections if the number of fields
        is less than 30 000 (batch_size).
        Returns
        -------
        -
        Parameters
        ----------
        coll : string
             : name of collection which is going to be transferred
        '''
        (r, attrs_details) = self.adjust_columns(coll)

        if self.tailing_from is not None or self.tailing_from_db is True:
            return

        if self.include_extra_props is True:
            docs = collection.get_by_name(self.mdb, coll)
        else:
            attr_source = [k for k, v in self.attrs_details.items()]
            docs = collection.get_by_name_reduced(self.mdb, coll, attr_source)

        # Start transferring docs
        nr_of_docs = docs.count()
        nr_of_transferred = 1000
        i = 0
        transferring = []
        for doc in docs:
            transferring.append(doc)
            try:
                if (i+1) % nr_of_transferred == 0 and i+1 >= nr_of_transferred:
                    if self.include_extra_props is True:
                        r.insert_config_bulk(
                            transferring,
                            self.attrs_details,
                            self.include_extra_props)
                    else:
                        r.insert_config_bulk_no_extra_props(
                            transferring,
                            self.attrs_details,
                            self.include_extra_props)
                    transferring = []
                if i + 1 == nr_of_docs and (i + 1) % nr_of_transferred != 0:
                    if self.include_extra_props is True:
                        r.insert_config_bulk(
                            transferring,
                            self.attrs_details,
                            self.include_extra_props)
                    else:
                        r.insert_config_bulk_no_extra_props(
                            transferring,
                            self.attrs_details,
                            self.include_extra_props)
                        logger.info(
                            "[EXTRACTOR] Finished collection %s: %d docs"
                            % (coll, i + 1))
                        transferring = []
            except Exception as ex:
                logger.error("""[EXTRACTOR] Transfer unsuccessful. %s""" % ex)
            if (i+1) % (nr_of_transferred*10) == 0:
                logger.info("""
                   [EXTRACTOR] Transferred %d from collection %s
                   """ % (i+1, coll))
            i += 1

    def insert_multiple(self, docs, r, coll, unset=[]):
        '''
        Transfers multiple documents with different fields
        (not whole collections).
        Parameters
        ----------
        doc : dict
            : document
        r : Relation
            relation in PG
        coll : string
             : collection name
        Returns
        -------
        -

        Raises
        ------
        Example
        -------
        '''
        (
            att_new,
            att_orig,
            types,
            name_rel,
            type_x_props_pg
        ) = cp.config_fields(self.coll_def, coll)

        # TODO: check if this is necessary:
        if types == []:
            return

        # Adding extra properties to inserted/updated row is necessary
        # because this attribute is not part of the original document
        # and anything that is not defined in the collection.yml file
        # will be pushed in this value. This function will also create
        # a dictionary which will contain all the information about
        # the attribute before and after the conversion.

        self.attrs_details = self.prepare_attr_details(
            att_new, att_orig, types, type_x_props_pg)
        # TODO remove this stuff with the extra props
        try:
            if self.include_extra_props is True:
                r.insert_config_bulk(
                    docs, self.attrs_details, self.include_extra_props, unset)
            else:
                r.insert_config_bulk_no_extra_props_tailed(
                    docs, self.attrs_details, self.include_extra_props, unset)
        except Exception as ex:
            logger.error("""
            [EXTRACTOR] Transferring to %s was unsuccessful.
            Exception: %s
            """ % (
                r.name_rel, ex))
            logger.error("%s\n" % docs)

    def update_multiple(self, docs, r, coll, unset=[]):
        '''
        Transfers multiple documents with different fields.
        Parameters
        ----------
        doc : dict
            : document
        r : Relation
            relation in PG
        coll : string
             : collection name
        Returns
        -------
        -

        Raises
        ------
        Example
        -------
        '''

        (attrs_new, attrs_original, types, relation_name,
         type_x_props_pg) = cp.config_fields(self.coll_def, coll)
        if types == []:
            return
        # Adding extra properties to inserted/updated row is necessary
        # because this attribute is not part of the original
        # document and anything that is not defined in the
        # collection.yml file will be pushed in this value.
        # This function will also create a dictionary which will
        # contain all the information
        # about the attribute before and after the conversion.

        self.attrs_details = self.prepare_attr_details(
            attrs_new, attrs_original, types, type_x_props_pg)
        # TODO remove this stuff with the extra props.
        try:
            if self.include_extra_props is True:
                r.insert_config_bulk(
                    docs, self.attrs_details, self.include_extra_props, unset)
            else:
                r.insert_config_bulk_no_extra_props_tailed(
                    docs, self.attrs_details, self.include_extra_props, unset)
        except Exception as ex:
            logger.error("""
            [EXTRACTOR] Transferring to %s was unsuccessful. Exception: %s
            """ % (
                r.relation_name, ex))
            logger.error('%s\n' % docs)

    def prepare_attr_details(self,
                             attrs_conf,
                             attrs_mdb,
                             types_conf,
                             type_x_props_pg=None):
        '''
        Adds extra properties field.
        This field needs to be added like this because it is not
        part of the original document.
        It can also have any type.
        Returns
        -------
        attrs_details : list
                      : attribute details with extra property

        Parameters
        ----------
        attrs_conf : list
                    : attribute names from config file
        attrs_mdb : list
                    : field names of MongoDB document
        types_conf : list
                    : types from config files
        extra_props_type : string
                    : type of the extra property
        Example
        -------
        attrs_new = [kit_cat, birdy_bird]
        attrs_original = [kitCat, birdyBird]
        types = ['text', 'text']
        extra_props_type = 'jsonb'
        res = append_extra_props(
            attrs_new, attrs_original, types, extra_props_type
        )
        '''
        if self.include_extra_props is True:

            attrs_conf.append(name_extra_props_pg)
            attrs_mdb.append(name_extra_props_mdb)
            types_conf.append(type_x_props_pg)

        self.attrs_details = {}
        for i in range(0, len(attrs_mdb)):
            details = {}
            details["name_conf"] = attrs_conf[i]
            details["type_conf"] = types_conf[i]
            details["value"] = None
            self.attrs_details[attrs_mdb[i]] = details
        return self.attrs_details

    def adjust_columns(self, coll):
        """
        Adds or removes extra properties if necessary and updates column types.
        """
        # get data from collection map
        (attrs_new,
         attrs_original,
         types,
         relation_name,
         type_x_props
         ) = cp.config_fields(self.coll_def, coll)
        if types == []:
            return

        r = relation.Relation(self.pg, self.schema, relation_name)

        # This dict contains all the necessary information about the
        # Mongo fields, Postgres columns and their types
        self.attrs_details = {}
        attrs_mdb = attrs_original
        attrs_conf = attrs_new
        types_conf = types

        if self.include_extra_props is True:
            attrs_conf.append(name_extra_props_pg)
            types_conf.append(type_x_props)
            attrs_mdb.append(name_extra_props_mdb)

            # Add column extra_props to table if it does not exist
            # table.add_column(self.pg, self.schema, r.relation_name,
            # name_extra_props_pg, type_x_props)
        # else:
        #   # Remove column extra_props from table if it exists
        #   table.remove_column(self.pg, r.relation_name, name_extra_props_pg)

        for i in range(len(attrs_mdb)):
            details = {}
            details["name_conf"] = attrs_conf[i]
            details["type_conf"] = types[i]
            details["value"] = None
            self.attrs_details[attrs_mdb[i]] = details

        # TODO insert function call here
        # Check if changing type was unsuccessful.
        type_update_failed = r.udpate_types(self.attrs_details)

        if type_update_failed is not None:
            for tuf in type_update_failed:
                name_pg = tuf[0]
                name_mdb = [
                    attr for attr in self.attrs_details if self.attrs_details[attr]["name_conf"] == name_pg][0]
                type_orig = tuf[1].lower()
                type_new = self.attrs_details[name_mdb]["type_conf"].lower()
                self.attrs_details[name_mdb]["type_conf"] = type_orig
                logger.warn("""
                [EXTRACTOR] Type conversion failed for column '%s'.
                Skipping conversion %s -> %s.""" %
                            (name_pg, type_orig, type_new))

        return r, self.attrs_details
