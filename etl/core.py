import time
import sys
from datetime import datetime
import os
from etl.extract import collection, extractor, tailer, init_mongo as mongodb, transfer_info
from etl.load import schema, init_pg as postgres, listener
from etl.monitor import logger
from etl.transform import config_parser as cp
import pkg_resources
import _thread
from threading import Thread


class NotificationThread(Thread):
    conn = None
    new = False

    def __init__(self, conn):
        Thread.__init__(self)
        self.conn = conn

    def run(self):
        self.listen()

    def listen(self):
        """
        Listens for changes from the channel.
        Parameters
        ----------
        db :    pgection
        Returns
        -------
        -
        Example
        -------
        listen(pg)
        """

        channel = 'purr'
        cmd = 'LISTEN %s;' % channel
        db = self.conn.conn
        db.cursor().execute(cmd)
        seconds_passed = 0
        try:
            while 1:
                db.commit()
                db.poll()
                db.commit()
                while db.notifies:
                    notify = db.notifies.pop()
                    logger.info("[LISTENER] New notification: pid=%s channel=%s payload=%s" % (
                        notify.pid, notify.channel, notify.payload))
                    self.new = True
        except Exception as ex:
            logger.error("Details: %s" % ex)
            return


class TransferThread(Thread):
    new = False

    def __init__(self, settings, coll_config, pg, mongo):
        Thread.__init__(self)
        self.settings = settings
        self.coll_config = coll_config
        self.pg = pg
        self.mongo = mongo
        self.t = tailer.Tailer(self.pg, self.mongo,
                               self.settings["postgres"], self.settings, self.coll_config)
        # initialize extractor
        self.ex = extractor.Extractor(
            self.pg, self.mongo.conn, self.settings["postgres"], self.settings, self.coll_config)

    def run(self):
        # transfer_and_sync(self.settings, self.coll_config, self.pg, self.mongo)
        # collections which will be transferred
        setup_pg = self.settings["postgres"]
        setup_mdb = self.settings["mongo"]

        collections = cp.config_collection_names(self.coll_config)

        if collections is None:
            logger.error(
                "[CORE] No collections found. Check your collection names in the setup file.")
            return

        schema.create(self.pg, setup_pg["schema_name"])

        # after extracting all collections tailing will start from this timestamp
        start_date_time = datetime.utcnow()

        if setup_pg["schema_reset"] is True:
            schema.reset(self.pg, setup_pg["schema_name"])

        transfer_info.create_stat_table(self.pg, setup_pg["schema_name"])
        transfer_info.create_coll_map_table(
            self.pg, setup_pg["schema_name"], self.coll_config)

        # Skip collection transfer if started in tailing mode.
        if self.settings["tailing_from_db"] is False and self.settings["tailing_from"] is None:
            if self.ex.typecheck_auto is True:
                self.ex.transfer_auto(collections)
            else:
                self.ex.transfer_conf(collections)
        self.tail(start_date_time)

    def tail(self, start_date_time=None):
        self.t.stop_tailing = False

        if self.settings["tailing"] is True:
            logger.info("Starting standard tailing.")
            self.t.start(start_date_time)

        elif self.settings["tailing_from"] is not None:
            logger.info("Starting tailing from provided timestamp.")
            self.t.start(settings["tailing_from"])

        elif self.settings["tailing_from_db"] is True or start_date_time is None:
            logger.info("Starting tailing from timestamp found in purr_info.")
            ts = transfer_info.get_latest_successful_ts(self.pg, 'public')
            latest_ts = int((list(ts)[0])[0])
            self.t.start(latest_ts)
        else:
            self.pg.__del__()

    def tailing_stop(self):
        self.t.stop_tailing = True


def start(settings, coll_config):
    """
    Starts Purr.
    Returns
    -------
    -

    Parameters
    ----------    
    settings : dict
             : basic settings for both PG and MongoDB (connection strings, schema name)

    coll_config : dict
                : config file for collections

    TODO
    ----
    - create table with attributes and types
    """
    logger.info("Starting Purr v%s ..." %
                pkg_resources.require("purr")[0].version)

    logger.info("PID=%s" % os.getpid())

    setup_pg = settings["postgres"]
    setup_mdb = settings["mongo"]

    pg = postgres.PgConnection(setup_pg["connection"])
    mongo = mongodb.MongoConnection(setup_mdb)

    try:
        notification = NotificationThread(pg)
        notification.start()

        transfer = TransferThread(settings, coll_config, pg, mongo)
        transfer.start()
        while True:
            if notification.new is True:
                notification.new = False
                transfer.tailing_stop()
                # TODO: type changes
                time.sleep(5)
                transfer.settings["tailing"] = False
                transfer.settings["tailing_from_db"] = True
                transfer.tail()

        notification.join(3)  # wait for 3 seconds
        transfer.join(3)

    except Exception as ex:
        logger.error(
            "Error: unable to start listener thread. Details: %s" % ex)
        raise SystemExit()


# def transfer_and_sync(settings, coll_config, pg, mongo):
