# here comes everything with the oplog
import pymongo
import time
from datetime import datetime, timedelta
from bson import Timestamp

from etl.transform import relation
from etl.monitor import logger
from etl.extract import extractor, transfer_info

INSERT = "i"
UPDATE = "u"
DELETE = "d"


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
        self.setup_pg = setup_pg
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
            return False

    def transform_and_load_one(self, doc):
        """
        Gets the document and passes it to the corresponding function in order to exeucte command INSERT/UPDATE/DELETE 
        Parameters
        ----------
        doc :   dict
                document from MongoDB
        Example
        -------
        transform_and_load({"_id":ObjectId("5acf593eed101e0c1266e32b"))
        Return
        ------
        None
        """

        fullname = doc["ns"]
        table_name_mdb = fullname.split(".")[1]
        table_name_pg = self.coll_settings[table_name_mdb][":meta"][":table"]

        oper = doc["op"]
        doc_useful = {}
        doc_useful = doc["o"]
        '''
        Check if relation exists in the PG database. 
        Skip the document if the trelation does not exist.
        '''

        r = relation.Relation(self.pg, self.schema_name, table_name_pg)
        logger.info("[TAILER] [%s] [%s]: [%s]" % (
            table_name_mdb, oper, doc_useful
        ))
        try:
            if oper == INSERT:
                if self.typecheck_auto is False:
                    super().transfer_one(doc_useful, r, table_name_mdb)
                else:
                    r.insert(doc_useful)

            elif oper == UPDATE:
                unset = []
                if "$set" in doc_useful.keys():
                    doc_useful = doc_useful["$set"]
                if "$unset" in doc_useful.keys():
                    for k, v in doc_useful["$unset"].items():
                        unset.append(k)

                if "o2" in doc.keys():
                    if "_id" in doc["o2"].keys():
                        doc_useful["_id"] = doc["o2"]["_id"]
                        logger.info("[TAILER] [%s]: [%s]" % (oper, doc_useful))

                    if self.typecheck_auto is False:
                        super().transfer_one(doc_useful, r, table_name_mdb, unset)
                    else:
                        r.update(doc_useful, unset)
            elif oper == DELETE:
                r.delete(doc_useful)

        except Exception as ex:
            logger.error(
                "[TAILER] %s - %s\n Document: %s\n %s"
                % (oper, table_name_pg, doc_useful, ex)
            )

    def flush(self, docs, oper, r):
        """
        sends all the data which was collected for one 
        collection during tailing to Postgres
        """
        docs_useful = []

        if oper == INSERT:
            logger.info("[TAILER] Inserting %s documents" % (len(docs)))
            # logger.info("[TAILER] %s" % (docs))

            # TODO: add functionality which includes extra props
            for doc in docs:
                docs_useful.append(doc["o"])
            super().insert_multiple(docs_useful, r, docs[0]["coll_name"])

        elif oper == UPDATE:
            logger.info("[TAILER] Updating %s documents" % (len(docs)))
            # logger.info("%s" % (docs))
            r.created = True
            docs_id = []
            for doc in docs:
                already_updating = False
                unset = {}
                doc_useful = {}
                temp = doc["o"]

                if "o2" in doc.keys():
                    if "_id" in doc["o2"].keys():
                        doc_useful["_id"] = str(doc["o2"]["_id"])
                        if (doc_useful["_id"] in docs_id):
                            already_updating = True
                        else:
                            docs_id.append(str(doc_useful["_id"]))

                if "$set" in temp.keys():
                    doc_useful.update(temp["$set"])
                if "$unset" in temp.keys():
                    for k, v in temp["$unset"].items():
                        unset[k] = 'unset'
                    doc_useful.update(unset)
                # merging values with the same ID because there cannot be
                # multiple updates of the same row in one statement
                if already_updating is True:
                    for i in range(0, len(docs_useful)):
                        if docs_useful[i]["_id"] == doc_useful["_id"]:
                            docs_useful[i] = dict(docs_useful[i], **doc_useful)
                            break
                else:
                    docs_useful.append(doc_useful)
            super().update_multiple(docs_useful, r, docs[0]["coll_name"])

        elif oper == DELETE:
            logger.info("[TAILER] Deleting %s documents" % (len(docs)))
            # logger.info("%s" % (docs))
            ids = []
            for doc in docs:
                ids.append(doc["o"])
            r.delete(ids)

    def transform_and_load_many(self, docs_details):
        """
        Gets the document and passes it to the corresponding function in order to exeucte command INSERT/UPDATE/DELETE 
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

        r = relation.Relation(self.pg, self.schema_name, table_name_pg, True)

        oper = docs_details[0]["op"]
        logger.info("[TAILER] [%s] [%s]" % (
            table_name_mdb, oper
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

    def handle_one(self, doc, updated_at):
        try:
            self.transform_and_load_one(doc)

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
                    "[TAILER] Updated latest_successful_ts: %d" % t)
            return datetime.utcnow()

        except Exception as ex:
            logger.error(
                "[TAILER] Transfer failed for document: %s: %s"
                % (doc, ex)
            )

    def handle_multiple(self, docs):
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

    def start(self, dt=None):
        """
        Starts tailing the oplog and prints write operation records on command line.
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
        if dt is None:
            start = datetime.utcnow()
            now = start - timedelta(
                minutes=0, seconds=30, microseconds=start.microsecond
            )
        else:
            now = dt

        client = self.mdb.client
        oplog = client.local.oplog.rs

        # Start reading the oplog
        SECONDS_BETWEEN_FLUSHES = 55
        try:
            updated_at = datetime.utcnow()
            loop = False
            while True and self.stop_tailing is False:
                logger.info("""[TAILER] Details:
                In loop: %s
                Client: %s
                Oplog: %s
                Timestamp (dt): %s""" % (
                    loop, client, oplog, str(dt)))
                if loop is True:
                    res = transfer_info.get_latest_successful_ts(
                        self.pg, self.schema)
                    latest_ts = int((list(res)[0])[0])
                    dt = latest_ts
                    logger.info("[TAILER] Next time, bring more cookies.")
                    raise SystemExit()
                else:
                    loop = True

                # if there was a reconnect attempt then start tailing from specific timestamp from the db
                if self.pg.attempt_to_reconnect is True:
                    res = transfer_info.get_latest_successful_ts(
                        self.pg, self.schema)
                    latest_ts = int((list(res)[0])[0])
                    dt = latest_ts
                    self.pg.attempt_to_reconnect = False

                cursor = oplog.find(
                    {"ts": {"$gt": Timestamp(dt, 1)}},
                    cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                    oplog_replay=True,
                )
                logger.info("[TAILER] Started tailing from %s." % str(dt))
                logger.info("[TAILER] Timestamp: %s" % datetime.utcnow())

                docs = []
                while cursor.alive and self.pg.attempt_to_reconnect is False:
                    if self.stop_tailing is True:
                        logger.info("[TAILER] Stopping now but will be back soon.")
                        break
                    try:
                        for doc in cursor:
                            if doc["op"] != "n" and self.coll_in_map(doc["ns"]) is True:
                                # updated_at = self.handle_one(doc, updated_at)
                                docs.append(doc)
                        time.sleep(1)
                        seconds = datetime.utcnow().second
                        if (seconds > SECONDS_BETWEEN_FLUSHES/3 and len(docs) > 0):
                            logger.info("[TAILER] Flushing after %s seconds. Number of documents: %s" % (
                                seconds, len(docs)))
                            self.handle_multiple(docs)
                            docs = []
                    except Exception as ex:
                        logger.error("[TAILER] %s" % ex)
                cursor.close()
                continue

        except StopIteration as e:
            logger.error("[TAILER] Tailing was stopped unexpectedly: %s" % e)
        except KeyboardInterrupt:
            logger.error("[TAILER] Tailing was stopped by the user.")

    def stop(self):
        logger.info("[TAILER] Tailing is stopped due to schema change.")
        self.stop_tailing = True
