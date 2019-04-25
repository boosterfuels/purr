# here comes everything with the oplog
import pymongo
import time
from datetime import datetime
from bson import Timestamp

from etl.transform import relation
from etl.monitor import logger
from etl.extract import extractor, transfer_info
import json

INSERT = "i"
UPDATE = "u"
DELETE = "d"

CURR_FILE = "[TAILER]"


def prepare_docs_for_update(docs):
    docs_useful = []
    docs_id = []
    for doc in docs:
        # It is possible that multiple versions of one document
        # exist among these documents. they must be merged so they
        # can be sent Postgres together as one entry.
        merge_similar = False
        unset = {}
        doc_useful = {}
        temp = doc["o"]

        if "o2" in doc.keys():
            if "_id" in doc["o2"].keys():
                doc_useful["_id"] = str(doc["o2"]["_id"])
                if (doc_useful["_id"] in docs_id):
                    merge_similar = True
                else:
                    docs_id.append(str(doc_useful["_id"]))

        if "$set" in temp.keys():
            doc_useful.update(temp["$set"])
            for k, v in temp["$set"].items():
                if v is None:
                    unset[k] = "$unset"
        if "$unset" in temp.keys():
            for k, v in temp["$unset"].items():
                unset[k] = '$unset'
        if "$set" not in temp.keys() and "$unset" not in temp.keys():
            # case when the document was not updated
            # using a query, but the IDE e.g. Studio3T:
            doc_useful.update(temp)
            for k, v in temp.items():
                if v is None:
                    unset[k] = '$unset'
        doc_useful.update(unset)

        # merging values with the same ID because there cannot be
        # multiple updates of the same row in one statement
        if merge_similar is True:
            for i in range(0, len(docs_useful)):
                if docs_useful[i]["_id"] == doc_useful["_id"]:
                    docs_useful[i] = dict(docs_useful[i], **doc_useful)
                    break
        else:
            docs_useful.append(doc_useful)
    return docs_useful, merge_similar


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
            logger.error("%s Deatils %s" % (CURR_FILE, ex))
            return False

    def flush(self, docs, oper, r):
        """
        sends all the data which was collected for one
        collection during tailing to Postgres
        """
        docs_useful = []
        ids_log = []
        merged = False

        if oper == INSERT:
            logger.info("%s Inserting %s documents" % (CURR_FILE, len(docs)))

            # TODO: check extra props
            for doc in docs:
                ids_log.append(str(doc["o"]["_id"]))
                docs_useful.append(doc["o"])
            try:
                super().insert_multiple(docs_useful, r, docs[0]["coll_name"])
            except Exception as ex:
                logger.info(
                    """
                    %s Inserting multiple documents failed: %s.
                    Details: %s
                    """ % (CURR_FILE, docs, ex))

        elif oper == UPDATE:
            logger.info("%s Updating %s documents" % (CURR_FILE, len(docs)))
            r.created = True

            (docs_useful, merged) = prepare_docs_for_update(docs)
            for doc in docs_useful:
                ids_log.append(str(doc["_id"]))
            try:
                super().update_multiple(docs_useful, r, docs[0]["coll_name"])
            except Exception as ex:
                logger.info(
                    """%s Updating multiple documents failed: %s.
                    Details: %s""" % (CURR_FILE, docs, ex))

        elif oper == DELETE:
            logger.info("%s Deleting %s documents" % (CURR_FILE, len(docs)))
            ids = []
            for doc in docs:
                ids.append(doc["o"])
                ids_log.append(str(doc["o"]["_id"]))
            try:
                r.delete(ids)
            except Exception as ex:
                logger.info(
                    """%s Deleting multiple documents failed: %s.
                    Details: %s""" % (CURR_FILE, docs, ex))

        log_entries = []
        ts = time.time()
        for i in range(len(ids_log)):
            id = ids_log[i]
            row = [oper, r.relation_name, id, ts,
                   merged, json.dumps(docs_useful[i]) or None]
            log_row = tuple(row)
            log_entries.append(log_row)
        try:
            transfer_info.log_rows(self.pg, self.schema, log_entries)
        except Exception as ex:
            logger.error("%s Logging failed. Details: %s" % (CURR_FILE, ex))

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
        logger.info("%s [%s] [%s]" % (
            CURR_FILE, table_name_mdb, oper
        ))

        docs_with_equal_oper = []
        for doc_details in docs_details:
            if oper == doc_details["op"]:
                docs_with_equal_oper.append(doc_details)
            else:
                self.flush(docs_with_equal_oper, oper, r)
                oper = doc_details["op"]
                docs_with_equal_oper = [doc_details]
        self.flush(docs_with_equal_oper, oper, r)

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
                    "%s Updated latest_successful_ts: %d" % (CURR_FILE, t))

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
                        """%s Stopping. Next time, bring more cookies."""
                        % (CURR_FILE))
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
                logger.info("%s Started tailing from %s.\nCurrent timestamp: %s" %
                            (CURR_FILE, str(dt), datetime.utcnow()))

                docs = []
                while cursor.alive and self.pg.attempt_to_reconnect is False:
                    if self.stop_tailing is True:
                        logger.info(
                            "%s Meow" % (CURR_FILE))
                        break
                    try:
                        for doc in cursor:
                            col = doc["ns"]
                            op = doc["op"]
                            if op != "n" and self.coll_in_map(col) is True:
                                docs.append(doc)
                        time.sleep(1)
                        seconds = datetime.utcnow().second
                        if (seconds > SECONDS_BETWEEN_FLUSHES and len(docs) or len(docs) > 50):
                            logger.info("""
                            %s Flushing after %s seconds.
                            Number of documents: %s
                            """ % (CURR_FILE, seconds, len(docs)))
                            self.handle_multiple(docs, updated_at)
                            docs = []
                    except Exception as ex:
                        logger.error("%s Cursor error %s" % (CURR_FILE, ex))
                cursor.close()
                continue
        except StopIteration as e:
            logger.error("%s Tailing was stopped unexpectedly: %s" %
                         (CURR_FILE, e))

    def stop(self):
        logger.info("%s Tailing is stopped due to schema change." %
                    (CURR_FILE))
        self.stop_tailing = True
