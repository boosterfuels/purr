import psycopg2
from bson.json_util import dumps

from etl.load import init_pg as pg, table
from etl.monitor import logger
import datetime
from psycopg2.extras import execute_values

# Open a cursor to perform database operations


def create_type_notification(db, name):
    """
    Creates a notification with a payload and sends it to channel 'purr'.

    Parameters
    ----------
    Returns
    -------
    Example
    -------
    create_notification(db)
    """
    cmd = """CREATE OR REPLACE FUNCTION %s()
    RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('purr', 'type_change');
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    """ % name

    try:
        logger.info("[PROCEDURE] Creating procedure: %s" % name)
        db.execute_cmd(cmd)
        logger.info("%s" % cmd)

    except Exception as ex:
        logger.error("[PROCEDURE] Insert failed: %s" % ex)


def drop_type_notification(db, name):
    """
    Drops a notification with a payload and sends it to channel 'purr'.

    Parameters
    ----------
    Returns
    -------
    Example
    -------
    drop_notification(db)
    """
    cmd = "DROP FUNCTION IF EXISTS %s();" % name

    try:
        db.execute_cmd(cmd)
        logger.info("[PROCEDURE] Dropping procedure: %s" % name)
    except Exception as ex:
        logger.error("[PROCEDURE] Dropping procedure failed: %s" % ex)
