import time
import sys
from datetime import datetime

from etl.extract import collection, extractor, tailer, init_mongo as mongodb, transfer_info
from etl.load import schema, init_pg as postgres
from etl.monitor import logger
from etl.transform import config_parser as cp

def start(settings, coll_config):
    """
    Starts 
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

    setup_pg = settings["postgres"]
    setup_mdb = settings["mongo"]
    # setup_file["collections"] contains list of collections
    # which will be transferred
    collections = cp.config_collection_names(coll_config)
    
    if collections is None:
        logger.error("No collections found. Check your collection names in the setup file.")
        return

    pg = postgres.PgConnection(setup_pg)
    schema.create(pg, setup_pg["schema_name"])

    mongo = mongodb.MongoConnection(setup_mdb)
    ex = extractor.Extractor(pg, mongo.conn, setup_pg, settings, coll_config)

    start_date_time = datetime.utcnow()
    if settings["tailing"] is True:
        t = tailer.Tailer(pg, mongo, setup_pg, settings, coll_config)

    if setup_pg["schema_reset"] is True:
        schema.reset(pg, setup_pg["schema_name"])

    transfer_info.create_stat_table(pg, setup_pg["schema_name"])

    if ex.typecheck_auto is True:
        ex.transfer_auto(collections)
    else:
        ex.transfer_conf(collections)

    if settings["tailing"] is True:
        t.start(start_date_time)
    else:
        pg.__del__()
