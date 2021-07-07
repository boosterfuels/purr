# here comes everything with the oplog
import pymongo
import time
from datetime import datetime
from bson import Timestamp
from threading import Thread
from multiprocessing import Queue

from etl.transform import relation
from etl.monitor import logger
from etl.extract import extractor, transfer_info

INSERT = "insert"
UPDATE = "update"
DELETE = "delete"

CURR_FILE = "[TAILER]"

COLLECTION_THREADS = []
DATA_QUEUE = Queue()
stop_threads = False

def prepare_docs_for_update(coll_settings, docs):
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


def log_tailed_docs(pg, schema, docs_useful, ids_log, table_name, oper, merged):
    log_entries = []
    ts = time.time()
    logger.info("IDs: %s" % ids_log)
    if len(ids_log) != len(docs_useful) and oper != DELETE:
        logger.error("n(ids)=%s; n(docs_useful)=%s" %
                     (len(ids_log), len(docs_useful)))
    for i in range(len(docs_useful)):
        id = ids_log[i]
        doc = "no entry"
        try:
            if docs_useful[i] is not None and oper != DELETE:
                doc = str(docs_useful[i])
            else:
                doc = "Doc is NULL"
        except Exception as ex:
            logger.error("%s Converting log entry failed. Details: %s\n Document: " %
                         (CURR_FILE, ex))
            logger.error(docs_useful[i])
        row = [oper, table_name, id, ts,
               merged, doc]
        log_row = tuple(row)
        log_entries.append(log_row)
    try:
        transfer_info.log_rows(pg, schema, log_entries)
    except Exception as ex:
        logger.error("%s Logging failed. Details: %s" % (CURR_FILE, ex))
   
def run_collection(conn, coll, stop):
    cursor = conn[coll].watch([
        {'$match': {
            'operationType': { '$in': [UPDATE, INSERT, DELETE]}
            }}])

    for doc in cursor:
        col = doc["ns"]["db"] + "." + doc["ns"]["coll"]
        op = doc["operationType"]

        if op in [INSERT, UPDATE, DELETE]:
            DATA_QUEUE.put(doc)

        if stop(): 
            break

    logger.info("%s Thread %s stopped." % (CURR_FILE, coll))
    cursor.close()

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
            logger.error("%s Details %s" % (CURR_FILE, ex))
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
            logger.info("%s Inserting %s documents into '%s'" %
                        (CURR_FILE, len(docs), r.relation_name))

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
            logger.info("%s Updating %s documents in '%s'" %
                        (CURR_FILE, len(docs), r.relation_name))
            r.created = True

            coll_name = docs[0]["coll_name"]
            coll_settings = self.coll_settings[coll_name]
            (docs_useful, merged) = prepare_docs_for_update(
                coll_settings,
                docs
            )
            for doc in docs_useful:
                ids_log.append(str(doc["_id"]))
            try:
                super().update_multiple(docs_useful, r, docs[0]["coll_name"])
            except Exception as ex:
                logger.info(
                    """%s Updating multiple documents failed: %s.
                    Details: %s""" % (CURR_FILE, docs, ex))

        elif oper == DELETE:
            logger.info("%s Deleting %s documents from '%s'" %
                        (CURR_FILE, len(docs), r.relation_name))
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
                self.flush(docs_with_equal_oper, oper, r)
                oper = doc_details["op"]
                docs_with_equal_oper = [doc_details]
        self.flush(docs_with_equal_oper, oper, r)

    def handle_multiple(self, docs, updated_at):
        # group by name
        docs_grouped = {}
        for doc in docs:
            collection = doc["ns"]["db"] + "." + doc["ns"]["coll"]
            if collection not in docs_grouped.keys():
                docs_grouped[collection] = []

            useful_info_update = {}
            if doc["operationType"] == UPDATE:
                if "updateDescription" in doc.keys():
                    useful_info_update = doc["updateDescription"]
            elif doc["operationType"] == INSERT:
                if "fullDocument" in doc.keys():
                    useful_info_update = doc["fullDocument"]
                else:
                    useful_info_update = doc["o2"]

            set_dict = {}
            d = {
                "op": doc["operationType"],
                "db_name": doc["ns"]["db"],
                "coll_name": doc["ns"]["coll"],
                "o": doc["documentKey"],
                "_id": doc["documentKey"]["_id"],
                "o2": useful_info_update
            }
            if doc["operationType"] == UPDATE:
                d["o"]["$set"] = useful_info_update['updatedFields']
            d["o"]["_id"] = doc["documentKey"]["_id"]
            if doc["operationType"] == INSERT:
                d["o"] = useful_info_update
            d["o2"]["_id"] = doc["documentKey"]["_id"]
            
            docs_grouped[collection].append(d)

        for coll, docs_details in docs_grouped.items():
            self.transform_and_load_many(docs_details)
            # every 5 minutes update the timestamp because we need to continue
            # tailing in case of disconnecting from the PGDB
            diff = datetime.utcnow() - updated_at
            minutes_between_update = (
                diff.seconds//60) % 60
            if minutes_between_update > 5:
                t = int(datetime.utcnow().timestamp())
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

        SECONDS_BETWEEN_FLUSHES = 30

        conn = self.mdb.conn
        updated_at = datetime.utcnow()
        stop_threads = False

        try:
            logger.info("%s Started tailing from %s.\nCurrent utctimestamp: %s" %
                        (CURR_FILE, str(dt), datetime.utcnow()))

            for coll in self.coll_settings.keys():    
                found = False
                x = Thread(target=run_collection, args=(conn, coll, lambda: stop_threads))
                x.daemon = True
                COLLECTION_THREADS.append(x)
                x.start()

            while True and self.stop_tailing is False:

                if self.stop_tailing is True:
                    logger.info("%s Meow" % (CURR_FILE))
                    break
                try:
                    time.sleep(1)
                    seconds = datetime.utcnow().second
                    if ((seconds > SECONDS_BETWEEN_FLUSHES and DATA_QUEUE.qsize()) or (DATA_QUEUE.qsize() > 100)):
                        logger.info("""
                        %s Flushing after %s seconds.
                        Number of documents: %s
                        """ % (CURR_FILE, seconds, DATA_QUEUE.qsize()))
                        docs = []
                        while not DATA_QUEUE.empty():
                            docs.append(DATA_QUEUE.get())
                        self.handle_multiple(docs, updated_at)
                except Exception as ex:
                    logger.error("%s Cursor error: %s" % (CURR_FILE, ex))

            logger.error("%s Tailing was stopped." % (CURR_FILE))
            stop_threads = True
            for t in COLLECTION_THREADS:
                t.join()
            logger.info("%s Exiting..." % (CURR_FILE))

        except StopIteration as e: 
            logger.error("%s Tailing was stopped unexpectedly: %s" % (CURR_FILE, e))
            stop_threads = True
            for t in COLLECTION_THREADS:
                t.join()

    def stop(self):
        logger.info("%s Tailing is stopped due to schema change." % (CURR_FILE))
        self.stop_tailing = True
