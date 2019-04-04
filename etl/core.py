import time
import sys
from datetime import datetime
import os
from etl.extract import collection, extractor, tailer, transfer
from etl.extract import init_mongo as mongodb, transfer_info, notification
from etl.extract import collection_map as cm
from etl.load import schema, init_pg as postgres
from etl.monitor import logger
from etl.transform import config_parser as cp, type_checker as tc
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
             : basic settings for both PG and MongoDB
             (connection strings, schema name)

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

    cm.create_table(
        pg, coll_config, setup_pg["schema_name"])

    ex = extractor.Extractor(
        pg, mongo.conn, setup_pg, settings, coll_config)
    THREADS = []
    try:
        thr_notification = notification.NotificationThread(pg)
        THREADS.append(thr_notification)
        thr_notification.start()
        thr_transfer = transfer.TransferThread(
            settings, coll_config, pg, mongo, ex)
        THREADS.append(thr_transfer)
        thr_transfer.start()
        while True:
            time.sleep(2)
            if thr_notification.new is True:
                thr_transfer.tailing_stop()

                thr_notification.new = False
                thr_transfer.join(3)

                thr_transfer = transfer.TransferThread(
                    settings, coll_config, pg, mongo, ex)
                THREADS.append(thr_transfer)
                thr_transfer.settings["tailing"] = False
                thr_transfer.settings["tailing_from_db"] = True
                thr_transfer.schema_update()
                thr_transfer.start()
        thr_notification.join(3)  # wait for 3 seconds
        thr_transfer.join(3)

    except (KeyboardInterrupt, SystemExit):
        logger.error('[CORE] Stopping all threads.')
        for t in THREADS:
            t.stop()
    except Exception as ex:
        logger.error(
            "[CORE] Unable to start listener thread. Details: %s" % ex)
        raise SystemExit()


def generate_collection_map(settings_mdb):
    """
    TODO: 
    - add docs
    - disconnect from Mongo!
    """
    logger.info("Starting Purr v%s ..." %
                pkg_resources.require("purr")[0].version)

    logger.info("PID=%s" % os.getpid())
    mongo = mongodb.MongoConnection(settings_mdb)
    coll_map = cm.determine_types(mongo.conn, settings_mdb["db_name"])
    cm.create_file(coll_map)
    # mongo.disconnect()
