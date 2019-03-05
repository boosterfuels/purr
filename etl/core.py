import time
import sys
from datetime import datetime
import os
from etl.extract import collection, extractor, tailer, init_mongo as mongodb, transfer_info, transfer, notification
from etl.load import schema, init_pg as postgres
from etl.monitor import logger
from etl.transform import config_parser as cp
import pkg_resources


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

    ex = extractor.Extractor(
        pg, mongo.conn, setup_pg, settings, coll_config)
    try:

        thr_notification = notification.NotificationThread(pg)
        thr_notification.start()
        thr_transfer = transfer.TransferThread(
            settings, coll_config, pg, mongo, ex)
        thr_transfer.start()
        while True:
            time.sleep(2)
            if thr_notification.new is True:
                thr_transfer.tailing_stop()

                thr_notification.new = False
                thr_transfer.join(3)

                thr_transfer = transfer.TransferThread(
                    settings, coll_config, pg, mongo, ex)
                thr_transfer.settings["tailing"] = False
                thr_transfer.settings["tailing_from_db"] = True
                thr_transfer.schema_update()
                thr_transfer.start()
        thr_notification.join(3)  # wait for 3 seconds
        thr_transfer.join(3)

    except Exception as ex:
        logger.error(
            "[CORE] Unable to start listener thread. Details: %s" % ex)
        raise SystemExit()
