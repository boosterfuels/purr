from datetime import datetime
import time
import os
from etl.extract import extractor, transfer
from etl.extract import init_mongo as mongodb, notification
from etl.extract import collection_map as cm
from etl.load import init_pg as postgres
from etl.monitor import logger
import pkg_resources

global THREADS
THREADS = []


def start(settings, coll_map):
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

    coll_map : dict
                : config file for collections

    TODO
    ----
    - create table with attributes and types
    """

    logger.info("Starting Purr v%s ..." %
                pkg_resources.require("purr")[0].version)

    logger.info("PID=%s" % os.getpid())

    mode_tailing = False
    if settings["tailing"] or settings["tailing_from"] or settings["tailing_from"]:
        mode_tailing = True

    logger.info("TAILING=%s" % ("ON" if mode_tailing else "OFF"))

    setup_pg = settings["postgres"]
    setup_mdb = settings["mongo"]

    pg = postgres.PgConnection(setup_pg["connection"])
    mongo = mongodb.MongoConnection(setup_mdb)

    cm.create_table(
        pg, coll_map, setup_pg["schema_name"])

    ex = extractor.Extractor(
        pg, mongo.conn, setup_pg, settings, coll_map)

    start_date_time = datetime.utcnow()

    if mode_tailing:
        try:
            # first just transfer the data
            tailing_start = False
            thr_transfer = transfer.TransferThread(
                settings,
                coll_map,
                pg,
                mongo,
                ex,
                tailing_start
            )
            thr_transfer.start()
            # wait until thread is finished
            thr_transfer.join()
            handle_coll_map_changes(settings, coll_map, pg,
                                    mongo, ex, start_date_time)
        except (KeyboardInterrupt, SystemExit):
            logger.error('[CORE] Stopping transfer.')
            for t in THREADS:
                t.stop()
            raise SystemExit()

        except Exception as ex:
            logger.error(
                "[CORE] Unablex to start transfer thread. Details: %s" % ex)
            raise SystemExit()

    else:
        transfer.start(ex, coll_map)


def handle_coll_map_changes(settings, coll_map, pg, mongo, ex, dt):
    tailing_start = True
    try:
        thr_notification = notification.NotificationThread(pg)
        THREADS.append(thr_notification)
        thr_notification.start()

        thr_transfer = transfer.TransferThread(
            settings, coll_map, pg, mongo, ex, tailing_start, dt)
        THREADS.append(thr_transfer)
        thr_transfer.start()
        while True:
            time.sleep(2)
            if thr_notification.new is True:
                thr_transfer.tailing_stop()

                thr_notification.new = False
                thr_transfer.join(3)

                thr_transfer = transfer.TransferThread(
                    settings, coll_map, pg, mongo, ex, tailing_start, dt)
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
        raise SystemExit()
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
    coll_map = cm.create_map(mongo.conn, settings_mdb["db_name"])
    cm.create_file(coll_map)
    mongo.disconnect()
