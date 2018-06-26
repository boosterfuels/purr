import time
import sys
from datetime import datetime

from etl.extract import collection, extractor, tailer
from etl.load import schema
from etl.extract import init_mongo as mongodb
from etl.load import init_pg as postgres


def transfer_collections(collections, settings, coll_config):
    """
  Parameters
  ----------
  collections
  truncate
  drop
  Example
  -------
  transfer_collections(['Feedback', 'Vehicle', 'Customer', 'Terminal'])
  TODO
  ----
  - create table with attributes and types
  """

    setup_pg = settings["postgres"]
    setup_mdb = settings["mongo"]

    pg = postgres.PgConnection(setup_pg)
    mongo = mongodb.MongoConnection(setup_mdb)
    ex = extractor.Extractor(pg, mongo.conn, setup_pg, settings, coll_config)

    start_date_time = datetime.utcnow()
    if settings["tailing"] is True:
        t = tailer.Tailer(pg, mongo, setup_pg, settings, coll_config)

    if setup_pg["schema_reset"] is True:
        schema.reset(pg, setup_pg["schema_name"])
    if ex.typecheck_auto is True:
        ex.transfer_auto(collections)
    else:
        ex.transfer_bulk(collections)

    if settings["tailing"] is True:
        t.start(start_date_time)
    else:
        pg.__del__()


# def start_tailing(start_date_time, pg, mdb, setup_pg, settings, coll_config):
#   t = tailer.Tailer(pg, mdb, setup_pg, settings, coll_config)
#   t.start(start_date_time)
