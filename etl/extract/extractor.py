from etl.extract import collection
from etl.extract import collection_map as cm
from etl.load import table, schema
from etl.monitor import logger
from etl.extract import transfer_info
from etl.extract import extractor_helper as eh
import time
from etl.transform import relation, type_checker as tc, config_parser as cp

CURR_FILE = "[EXTRACTOR]"


class Extractor():
    """
    This is a class for extracting data from collections.
    """

    def __init__(self, pg, mdb, settings_pg, settings_general, coll_def):
        """Constructor for Extractor"""

        self.pg = pg
        self.mdb = mdb
        self.has_extra_props = settings_general['include_extra_props']
        try:
            self.has_extra_props = settings_general['include_extra_props']
        except KeyError:
            self.has_extra_props = False
        self.schema = settings_pg["schema_name"]
        self.truncate = settings_pg['table_truncate']
        self.drop = settings_pg['table_drop']
        self.tables_empty = self.truncate or self.drop
        self.settings_pg = settings_pg
        self.coll_def = coll_def
        self.tailing_from = settings_general['tailing_from']
        self.tailing_from_db = settings_general['tailing_from_db']
        self.coll_map_cur = cm.get_coll_map_from_db(self.pg)

    def column_convert(self, name_table, source, fields_cur, fields_new):
        """
        (1) Tries to convert the column
        (2) TODO: If (1) was not successful (PG could not
        convert the column), just rename it and add
        the column again so Purr can take care of it
        """
        for i in range(0, len(fields_new)):
            field = fields_new[i]
            if field[":source"] in source:
                for column, v in field.items():
                    if v is None:
                        type_old = fields_cur[i][":type"]
                        type_new = field[":type"]
                        if tc.is_convertable(type_old, type_new):
                            logger.info(
                                """%s table %s, column %s:
                                Type [%s] is convertable to [%s]""" % (
                                    CURR_FILE,
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
                                %s In table %s, column %s:
                                Type [%s] is NOT convertable to [%s]
                                """ % (
                                CURR_FILE,
                                name_table,
                                column,
                                type_old,
                                type_new))

    def column_add(self, source, added, name_coll, name_table, fields_new, cm):
        """
        Adds columns to a table and updates the coll_def.
        After that, it restarts the collection transfer
        so the changes would be picked up.
        """
        new_columns = []
        # TODO: add new columns
        for item in added:
            if item[":source"] not in source:
                for attribute, v in item.items():
                    if v is None:
                        self.coll_def[name_coll][":columns"] = fields_new
                        table.add_column(
                            self.pg,
                            self.schema,
                            name_table,
                            attribute,
                            item[":type"])
                        self.transfer_collections(cm[1], new_columns)

    def column_remove(self, source, removed, name_coll, name_table, fields_new):
        """
        Removes columns to a table and updates the coll_def.
        """
        for item in removed:
            if item[":source"] not in source:
                for attribute, v in item.items():
                    if v is None:
                        # update collection settings
                        self.coll_def[name_coll][":columns"] = fields_new
                        table.remove_column(
                            self.pg,
                            self.schema,
                            name_table,
                            attribute)

    def update_table_def(self, coll_map_cur, coll_map_new):
        """
        Updates tables' columns and collection definition (coll_def)
        """
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
                self.column_convert(
                    name_table,
                    source_persistent,
                    fields_cur,
                    fields_new
                )

            self.column_add(source_persistent, added,
                            name_coll, name_table, fields_new,
                            coll_map_cur[i])

            self.column_remove(source_persistent, removed,
                               name_coll, name_table, fields_new)

    def table_track(self, coll_map_cur, coll_map_new):
        """
        coll_map_cur : list
                     : current collection map
        coll_map_new : list
                     : new collection map
        Update the extractor object's collection map and
        starts tracking collections (data transfer).

        TODO: take care of extra props type (JSONB)
        """
        logger.info("%s Adding new collection" %
                    CURR_FILE)
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
            self.transfer_collections(name_coll)

    def table_untrack(self, coll_map_cur, coll_map_new):
        tables_cur = [x[2] for x in coll_map_cur]
        tables_remaining = [x[2] for x in coll_map_new]

        tables_to_drop = [x for x in tables_cur if x not in tables_remaining]
        colls_to_remove = [x[1]
                           for x in coll_map_cur if x[2] in tables_to_drop]

        logger.info("%s Stop syncing collections %s." %
                    (
                        CURR_FILE,
                        ", ".join(colls_to_remove)))

        for coll in colls_to_remove:
            self.coll_def.pop(coll, None)

    def update_coll_map(self):
        # TODO: get types from pg
        logger.info("%s Updating schema from purr_collection_map" %
                    CURR_FILE)

        coll_map_cur = self.coll_map_cur
        coll_map_new = cm.get_coll_map_from_db(self.pg)

        if coll_map_cur == coll_map_new:
            logger.info("%s Database schema is not changed" %
                        CURR_FILE)
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

    def cleanup(self):
        if self.drop:
            table.drop(self.pg, self.schema, relation_names)
        elif self.truncate:
            table.truncate(self.pg, self.schema, relation_names)

    def start(self, collections):
        """
        Starts with transferring whole collections if the number of fields
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
        # check if collections exist
        coll_names = collection.check(self.mdb, collections)
        if len(coll_names) == 0:
            logger.info('%s No collections to transfer.' % CURR_FILE)
            return

        relation_names = []
        for coll in coll_names:
            relation_names.append(tc.snake_case(coll))

        self.cleanup()
        schema.create(self.pg, self.schema)

        for coll in coll_names:
            self.transfer_collections(coll)

    def transfer_collections(self, coll, new_columns=[]):
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

        if self.tailing_from is not None or self.tailing_from_db is True:
            return
        attr_details = eh.get_attr_details(
            self.coll_def, coll, self.has_extra_props)

        docs = eh.get_docs(
            self.mdb,
            self.has_extra_props,
            coll,
            attr_details,
            new_columns
        )

        nr_of_docs = docs.count()

        MAX_N_ROWS = self.settings_pg["n_rows"]

        (n_process, size_chunk) = eh.get_n_process(
            nr_of_docs, MAX_N_ROWS)

        pg_conns = eh.init_connections(
            n_process,
            self.settings_pg["connection"]
        )

        # the documents will be divided into n chunks
        # one chunk will be transferred by one process
        chunks = eh.init_chunks(n_process)

        # init relations object for each connection
        relations = []

        for conn in pg_conns:
            r, attr_details = self.init_relation(coll, conn)
            relations.append(r)

        # Start transferring docs
        i = 0
        j = 0
        transfer_start = time.time()
        for doc in docs:
            is_last_doc = (i + 1 == nr_of_docs)
            chunks[j].append(doc)

            # if the current chunk is full, move to the next one
            if len(chunks[j]) == size_chunk:
                j = j+1

            try:
                # we reached the maximum number of docs we can transfer at once OR
                # we reached the last document therefore we start pumping
                if (i+1) % MAX_N_ROWS == 0 or is_last_doc:
                    processes = eh.init_processes(
                        pg_conns,
                        chunks,
                        attr_details,
                        self.has_extra_props,
                        relations
                    )

                    eh.start_processes(processes)
                    eh.finish_processes(processes)
                    j = 0
                    chunks = eh.init_chunks(n_process)

                if is_last_doc is True:
                    logger.info(
                        "%s Finished collection %s: %d docs"
                        % (CURR_FILE,
                            coll, i + 1))

            except Exception as ex:
                logger.error("""%s Transfer unsuccessful. %s""" % (
                    CURR_FILE,
                    ex))

            if (i+1) % (MAX_N_ROWS) == 0:
                logger.info("""%s %d/%d (%s)""" % (
                    CURR_FILE,
                    i+1,
                    nr_of_docs,
                    coll))
            i += 1

        # log and full vacuum
        transfer_details = eh.get_transfer_details(
            self.tables_empty,
            transfer_start,
            time.time()
        )
        vacuum_details = eh.get_vacuum_details(relations[0])

        eh.log(relations[0],
               nr_of_docs,
               [transfer_details, vacuum_details]
               )
        eh.close_connections(pg_conns)

        # remove unused objects
        for r in relations:
            del r

    def insert_multiple(self, docs, r, coll):
        '''
        Used by [TAILER]
        Calls update_multiple when tailing which will UPSERT
        the documents. 
        Plans: create a function that will INSERT 
        because it is faster than UPSERT  
        Details: update_multiple(self, docs, r, coll)
        '''
        self.update_multiple(docs, r, coll)

    def update_multiple(self, docs, r, coll):
        '''
        Upserts multiple documents with different fields.
        Used by [TAILER]
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
        attr_details = eh.get_attr_details(
            self.coll_def, coll, self.has_extra_props
        )
        tailing = True
        try:
            r.insert(docs, attr_details,
                     self.has_extra_props, tailing)
        except Exception as ex:
            logger.error("""
            %s Transferring to %s was unsuccessful. Exception: %s
            """ % (
                CURR_FILE,
                r.relation_name, ex))
            logger.error('%s\n' % docs)

    def init_relation(self, coll, pg):
        """
        Initializes relation object
        Adds or removes extra properties if necessary and updates column types
        for a collection.
        Changes the attribute details (attr_details)

        Parameters
        ----------
        coll : string
             : name of the collection


        Returns
        ----------
        r : object
          : the relation object

        """
        # get data from the collection map
        (attrs_new,
         attrs_original,
         types,
         type_x_props
         ) = cp.config_fields(self.coll_def, coll)

        if types == []:
            return

        relation_name = cp.get_relation_name(self.coll_def, coll)
        r = relation.Relation(pg, self.schema, relation_name)
        # add extra properties if necessary
        attr_details = eh.add_extra_props(
            attrs_original, attrs_new, types, self.has_extra_props)

        # Check if changing type was unsuccessful. TODO
        attr_details = eh.handle_failed_type_update(r, attr_details)
        return (r, attr_details)
