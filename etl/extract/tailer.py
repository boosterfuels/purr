# here comes everything with the oplog
import pymongo
import time
from datetime import datetime
from bson import Timestamp

from etl.transform import relation
from etl.monitor import logger
from etl.extract import extractor, transfer_info

INSERT = "i"
UPDATE = "u"
DELETE = "d"

CURR_FILE = "[TAILER]"


def fuse(docs_useful, doc_useful, merge_similar):
    """ 
    Merging values with the same ID because there cannot be
    multiple updates of the same row in one statement"""

    if merge_similar is True:
        for i in range(0, len(docs_useful)):
            if docs_useful[i]["_id"] == doc_useful["_id"]:
                docs_useful[i] = dict(docs_useful[i], **doc_useful)
                break
    # otherwise just append the document
    else:
        docs_useful.append(doc_useful)

    return docs_useful


def unset_values(doc, unset, action):
    """
    Unset values of a document.
    doc: the document
    unset: a dictionary that contains which 
    keys of the document need to be to unset 
    action: the action that affects the values
    """
    if action == '$set':
        if "$set" in doc.keys():
            for k, v in doc["$set"].items():
                if v is None:
                    unset[k] = "$unset"
    elif action == '$unset':
        if "$unset" in doc.keys():
            for k, v in doc["$unset"].items():
                unset[k] = '$unset'
    elif action == 'direct_update':
        for k, v in doc.items():
            if v is None:
                unset[k] = '$unset'
    return unset


def handleQueryUpdate(doc, doc_useful, temp, unset, docs_id, merge_similar):
    # This function handles oplog entries when updating a document
    # happened using a query.
    if "o2" in doc.keys():
        # check if the same document is updated multiple times
        if "_id" in doc["o2"].keys():
            doc_useful["_id"] = str(doc["o2"]["_id"])
            if (doc_useful["_id"] in docs_id):
                merge_similar = True
            else:
                docs_id.append(str(doc_useful["_id"]))

        # updated element by setting variables
        if "$set" in temp.keys():
            doc_useful.update(temp["$set"])

        # look for values to unset
        unset = unset_values(temp, unset, '$set')
        unset = unset_values(temp, unset, '$unset')

    return (doc_useful, unset, merge_similar)


def handleDirectUpdate(doc, doc_useful, temp, unset, coll_settings):
    # This function handles oplog entries when updating a document
    # happened using an IDE e.g. Studio3T:
    if "$set" not in temp.keys() and "$unset" not in temp.keys():
        logger.info("Direct update:")
        doc_useful.update(temp)
        fields = [x[":source"]
                  for x in coll_settings[":columns"]]
        for k in fields:
            if k == '_id':
                temp[k] = str(temp[k])
                doc_useful.update(temp)
            if k not in temp.keys():
                unset[k] = '$unset'
        unset = unset_values(temp, unset, 'direct_update')
    return (doc_useful, unset)


def modify_docs_before_update(coll_settings, docs):
    """
    Modifies documents based on the keys found in the oplog entry.
    An entry may (not necessarily) contain keys like $o2, $set and $unset.
    There are multiple situations when we need to unset a value:
    - if it has no value: null (None)
    - if the key appeared in $unset
    - if the key was left out and there was no $set/$unset
    """
    result = []
    docs_id = []
    for doc in docs:
        # It is possible that multiple versions of one document
        # exist among these documents. they must be merged so they
        # can be sent Postgres together as one entry.
        ids_equal = False
        unset = {}
        doc_to_append = {}
        temp = doc["o"]

        # updated using a query
        doc_to_append, unset, ids_equal = handleQueryUpdate(
            doc, doc_to_append, temp, unset, docs_id, ids_equal)

        # updated without a query (possibly using a client app like Studio3T)
        handleDirectUpdate(doc, doc_to_append, temp, unset, coll_settings)

        doc_to_append.update(unset)

        # whatever needs to be appended to result, just append it
        result = fuse(result, doc_to_append, ids_equal)

    return result, ids_equal


def log_tailed_docs(pg, schema, docs_useful, ids_log, table_name, oper, merged):
    log_entries = []
    ts = time.time()
    logger.info("IDs: %s" % ids_log)
    if len(ids_log) != len(docs_useful) and oper != 'd':
        logger.error("n(ids)=%s; n(docs_useful)=%s" %
                     (len(ids_log), len(docs_useful)))
    for i in range(len(docs_useful)):
        id = ids_log[i]
        doc = "no entry"
        try:
            if docs_useful[i] is not None and oper != 'd':
                doc = str(docs_useful[i])
            else:
                doc = "Doc is NULL"
        except Exception as ex:
            logger.error(
                """Converting log entry failed. Details: %s
                Document: """ %
                ex, CURR_FILE)
            logger.error(docs_useful[i])
        row = [oper, table_name, id, ts,
               merged, doc]
        log_row = tuple(row)
        log_entries.append(log_row)
    try:
        transfer_info.log_rows(pg, schema, log_entries)
    except Exception as ex:
        logger.error("Logging failed. Details: %s" % ex, CURR_FILE)


class Tailer(extractor.Extractor):
    """
    Class for extracting data from the oplog.
    """

    def __init__(self, pg, mdb, setup_pg, settings, coll_settings):
        """
        Parameters
        ----------
        pg : postgres connection
        mdb : mongo connection
        setup_pg : postgres specific settings from setup.yml
        settings : settings from setup.yml
        coll_settings : settings for each collection from collections.yml
        """
        extractor.Extractor.__init__(
            self, pg, mdb, setup_pg, settings, coll_settings)
        self.pg = pg
        self.schema = setup_pg["schema_name"]
        self.settings = settings
        self.coll_settings = coll_settings
        self.stop_tailing = False

    def coll_in_map(self, name):
        '''
        name: string;
            name of collection as 'name_db.name_coll', e.g. 'cat_db.Breeds'
        Checks if a collection exists in collections.yml.
        '''
        coll = name.split(".")[1]
        try:
            if coll in self.coll_settings.keys():
                return True
            else:
                return False
        except Exception as ex:
            logger.error("Details %s" % ex, CURR_FILE)
            return False

    def send_group(self, docs, oper, r):
        """
        sends all the data which was collected for one
        collection during tailing to Postgres
        """
        docs_useful = []
        ids_log = []
        merged = False

        if oper == INSERT:
            logger.info("Inserting %s documents into '%s'" %
                        (len(docs), r.relation_name), CURR_FILE)

            # TODO: check extra props
            for doc in docs:
                ids_log.append(str(doc["o"]["_id"]))
                docs_useful.append(doc["o"])
            try:
                super().insert_multiple(docs_useful, r, docs[0]["coll_name"])
            except Exception as ex:
                logger.info(
                    """
                    Inserting multiple documents failed: %s.
                    Details: %s
                    """ % (docs, ex), CURR_FILE)

        elif oper == UPDATE:
            logger.info("Updating %s documents in '%s'" %
                        (len(docs), r.relation_name), CURR_FILE)
            r.created = True

            coll_name = docs[0]["coll_name"]
            coll_settings = self.coll_settings[coll_name]
            (docs_useful, merged) = modify_docs_before_update(
                coll_settings,
                docs
            )
            for doc in docs_useful:
                ids_log.append(str(doc["_id"]))
            try:
                super().update_multiple(docs_useful, r, docs[0]["coll_name"])
            except Exception as ex:
                logger.info(
                    """Updating multiple documents failed: %s.
                    Details: %s""" % (docs, ex), CURR_FILE)

        elif oper == DELETE:
            logger.info("Deleting %s documents from '%s'" %
                        (len(docs), r.relation_name), CURR_FILE)
            ids = []
            for doc in docs:
                ids.append(doc["o"])
                ids_log.append(str(doc["o"]["_id"]))
            try:
                r.delete(ids)
            except Exception as ex:
                logger.info(
                    """Deleting multiple documents failed: %s.
                    Details: %s""" % (docs, ex), CURR_FILE)

        log_tailed_docs(
            self.pg, self.schema,
            docs_useful, ids_log,
            r.relation_name, oper, merged
        )

    def transform_and_load_many(self, docs_details):
        """
        Gets the document and passes it to the corresponding
        function in order to exeucte command INSERT/UPDATE/DELETE
        Parameters
        ----------
        docs :  list of documents and operations
                document from MongoDB
        Example
        -------
        transform_and_load({"_id":ObjectId("5acf593eed101e0c1266e32b"))
        Return
        ------
        None
        """
        table_name_mdb = docs_details[0]["coll_name"]
        table_name_pg = self.coll_settings[table_name_mdb][":meta"][":table"]

        r = relation.Relation(self.pg, self.schema, table_name_pg, True)

        oper = docs_details[0]["op"]

        docs_with_equal_oper = []
        for doc_details in docs_details:
            if oper == doc_details["op"]:
                docs_with_equal_oper.append(doc_details)
            else:
                self.send_group(docs_with_equal_oper, oper, r)
                oper = doc_details["op"]
                docs_with_equal_oper = [doc_details]
        self.send_group(docs_with_equal_oper, oper, r)
        del r

    def handle_multiple(self, docs, updated_at):
        # group by name
        docs_grouped = {}
        for doc in docs:
            if doc["ns"] not in docs_grouped.keys():
                docs_grouped[doc["ns"]] = []

            useful_info_update = {}
            if "o2" in doc.keys():
                useful_info_update = doc["o2"]

            docs_grouped[doc["ns"]].append({
                "op": doc["op"],
                "db_name": doc["ns"].split(".")[0],
                "coll_name": doc["ns"].split(".")[1],
                "o": doc["o"],
                "o2": useful_info_update
            })

        for coll, docs_details in docs_grouped.items():
            self.transform_and_load_many(docs_details)
            # every 5 minutes update the timestamp because we need to continue
            # tailing in case of disconnecting from the PGDB
            diff = datetime.utcnow() - updated_at
            minutes_between_update = (
                diff.seconds//60) % 60
            if minutes_between_update > 5:
                t = int(time.time())
                transfer_info.update_latest_successful_ts(
                    self.pg, self.schema, t
                )
                logger.info(
                    "Updated latest_successful_ts: %d" % t, CURR_FILE)

    def start(self, dt=None):
        """
        Starts tailing the oplogs.
        Parameters
        ----------
        dt: datetime
            If datetime is None, tailing starts from the current timestamp.
        Example
        -------
        (1)
        start()

        (2)
        start(dt)
        """
        client = self.mdb.client
        oplog = client.local.oplog.rs

        # Start reading the oplog
        SECONDS_BETWEEN_FLUSHES = 30
        try:
            updated_at = datetime.utcnow()
            loop = False
            while True and self.stop_tailing is False:
                if loop is True:
                    res = transfer_info.get_latest_successful_ts(
                        self.pg, self.schema)
                    latest_ts = int((list(res)[0])[0])
                    dt = latest_ts
                    logger.error(
                        """Stopping. Next time, bring more cookies.""",
                        CURR_FILE)
                    raise SystemExit
                else:
                    loop = True

                # if there was a reconnect attempt then start tailing from
                # specific timestamp from the db
                if self.pg.attempt_to_reconnect is True:
                    res = transfer_info.get_latest_successful_ts(
                        self.pg, self.schema)
                    dt = int((list(res)[0])[0])
                    self.pg.attempt_to_reconnect = False

                cursor = oplog.find(
                    {"ts": {"$gt": Timestamp(dt, 1)}},
                    cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                    oplog_replay=True,
                )
                if type(dt) is int:
                    dt = datetime.utcfromtimestamp(
                        dt).strftime('%Y-%m-%d %H:%M:%S')
                logger.info("Started tailing from %s.\nCurrent timestamp: %s" %
                            (str(dt), datetime.utcnow()), CURR_FILE)

                docs = []
                while cursor.alive and self.pg.attempt_to_reconnect is False:
                    if self.stop_tailing is True:
                        logger.info(
                            "Tailing is stopped. Meow", CURR_FILE)
                        break
                    try:
                        for doc in cursor:
                            col = doc["ns"]
                            op = doc["op"]
                            if op != "n" and self.coll_in_map(col) is True:
                                docs.append(doc)
                        time.sleep(1)
                        seconds = datetime.utcnow().second
                        if ((seconds > SECONDS_BETWEEN_FLUSHES and len(docs)) or (len(docs) > 100)):
                            logger.info("""
                            Sending group after %s seconds.
                            Number of documents: %s
                            """ % (seconds, len(docs)), CURR_FILE)
                            self.handle_multiple(docs, updated_at)
                            docs = []
                    except Exception as ex:
                        logger.error("Cursor error: %s" % ex, CURR_FILE)
                cursor.close()
                continue
        except StopIteration as e:
            logger.error("Tailing was stopped unexpectedly: %s" %
                         e, CURR_FILE)

    def stop(self):
        logger.info("Tailing is stopped due to schema change.", CURR_FILE)
        self.stop_tailing = True
