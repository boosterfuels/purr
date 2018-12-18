import time
import sys
from datetime import datetime
import os
from etl.extract import collection, extractor, tailer, init_mongo as mongodb, transfer_info
from etl.load import schema, init_pg as postgres
from etl.monitor import logger
from etl.transform import config_parser as cp

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
    logger.info("Starting Purr v0.1.5 ...")
    logger.info("PID=%s" % os.getpid())
    setup_pg = settings["postgres"]
    setup_mdb = settings["mongo"]
    # collections which will be transferred
    collections = cp.config_collection_names(coll_config)
    
    if collections is None:
        logger.error("[CORE] No collections found. Check your collection names in the setup file.")
        return

    pg = postgres.PgConnection(setup_pg["connection"])
    schema.create(pg, setup_pg["schema_name"])

    mongo = mongodb.MongoConnection(setup_mdb)

    # initialize extractor
    ex = extractor.Extractor(pg, mongo.conn, setup_pg, settings, coll_config)

    # after extracting all collections tailing will start from this timestamp
    start_date_time = datetime.utcnow()

    if setup_pg["schema_reset"] is True:
        schema.reset(pg, setup_pg["schema_name"])

    transfer_info.create_stat_table(pg, setup_pg["schema_name"])
    
    # Skip collection transfer if started in tailing mode.
    if settings["tailing_from_db"] is False and settings["tailing_from"] is None:
        if ex.typecheck_auto is True:
            ex.transfer_auto(collections)
        else:
            ex.transfer_conf(collections)

    if settings["tailing"] is True:
        t = tailer.Tailer(pg, mongo, setup_pg, settings, coll_config)
        logger.info("Starting standard tailing.")
        t.start(start_date_time)
        
    elif settings["tailing_from"] is not None:
        logger.info("Starting tailing from provided timestamp.")
        t = tailer.Tailer(pg, mongo, setup_pg, settings, coll_config)
        t.start(settings["tailing_from"])

    elif settings["tailing_from_db"] is True:
        logger.info("Starting tailing from timestamp found in purr_info.")
        t = tailer.Tailer(pg, mongo, setup_pg, settings, coll_config)
        ts = transfer_info.get_latest_successful_ts(pg, 'public')
        latest_ts = int((list(ts)[0])[0])
        t.start(latest_ts)
    else:
        pg.__del__()
