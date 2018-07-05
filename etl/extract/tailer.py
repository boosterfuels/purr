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
        extractor.Extractor.__init__(self, pg, mdb, setup_pg, settings, coll_settings)
        self.tailing = False
        self.pg = pg
        self.schema = setup_pg["schema_name"]

    def transform_and_load(self, doc):
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

        oper = doc["op"]
        doc_useful = {}
        doc_useful = doc["o"]

        try:
            table_name_pg = self.coll_settings[table_name_mdb][":meta"][":table"]
        except Exception as ex:
            return

        r = relation.Relation(self.pg, self.schema_name, table_name_pg)
        if r.exists() is False:
            return

        if oper == INSERT:
            try:
                if self.typecheck_auto is False:
                    super().transfer_doc(doc_useful, r, table_name_mdb)
                else:
                    r.insert(doc_useful)
            except Exception as ex:
                logger.error(
                    "[TAILER] Insert into %s failed:\n Document: %s\n %s"
                    % (table_name_pg, doc_useful, ex)
                )

        elif oper == UPDATE:
            if "$set" in doc_useful.keys():
                doc_useful = doc_useful["$set"]

            if "o2" in doc.keys():
                if "_id" in doc["o2"].keys():
                    doc_useful["_id"] = doc["o2"]["_id"]
            try:
                if self.typecheck_auto is False:
                    super().transfer_doc(doc_useful, r, table_name_mdb)
                else:
                    r.update(doc_useful)
            except Exception as ex:
                logger.error(
                    "[TAILER] Update of %s failed:\n Document: %s\n %s"
                    % (table_name_pg, doc_useful, ex)
                )
        elif oper == DELETE:
            try:
                r.delete(doc_useful)
            except Exception as ex:
                logger.error(
                    "[TAILER] Delete from %s failed: \n Document: %s\n %s"
                    % (table_name_pg, doc_useful, ex)
                )

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
        self.tailing = True

        client = self.mdb.client
        oplog = client.local.oplog.rs

        # Start reading the oplog
        temp = {}
        try:
            updated = datetime.utcnow()
            while True:
                # if there was a reconnect attempt then start tailing from specific timestamp from the db
                if self.pg.attempt_to_reconnect is True:
                    res = transfer_info.get_latest_successful_ts(self.pg, self.schema)
                    latest_ts = int((list(res)[0])[0])
                    dt = latest_ts
                    self.pg.attempt_to_reconnect = False

                logger.info(
                    "[TAILER] Started tailing from %s." % str(dt)
                )
                cursor = oplog.find(
                    {"ts": {"$gt": Timestamp(dt, 1)}},
                    cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                    oplog_replay=True,
                )

                while cursor.alive and self.pg.attempt_to_reconnect is False:
                    for doc in cursor:
                        if doc["op"] != "n":
                            temp = doc["o"]
                            try:
                                self.transform_and_load(doc)
                                # every minute update the timestamp because
                                # we need to start tailing from somewhere 
                                # in case of disconnecting from the PGDB
                                diff = datetime.utcnow() - updated
                                minutes_between_update = (diff.seconds//60)%60
                                if minutes_between_update > 2:
                                    logger.info("[TAILER] Updating purr_info...")
                                    transfer_info.update_latest_successful_ts(
                                        self.pg, self.schema, int(time.time())
                                    )
                                    updated = datetime.utcnow()

                            except Exception as ex:
                                logger.error(
                                    "[TAILER] Transfer failed for document: %s: %s"
                                    % (temp, ex)
                                )
                    time.sleep(1)
                cursor.close()
                continue

        except StopIteration as e:
            logger.error("[TAILER] Tailing was stopped unexpectedly: %s" % e)
        except KeyboardInterrupt:
            logger.error("[TAILER] Tailing was stopped by the user.")
