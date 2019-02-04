import psycopg2
from bson.json_util import loads, dumps

from etl.load import init_pg as pg, table
from etl.monitor import logger
import datetime
from psycopg2.extras import execute_values

# Open a cursor to perform database operations


def create_type_notification(db):
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

    name = 'notify_trigger'
    cmd = """
    -- Trigger notification for messaging to PG Notify
    CREATE FUNCTION %s() RETURNS trigger AS $trigger$
    BEGIN
      PERFORM pg_notify('purr', 'schemachange');
    END;
    $trigger$ LANGUAGE plpgsql;
    """ % name

    try:
        db.execute_cmd(cmd)
        logger.info("[PROCEDURE] Creating procedure: %s" % name)
    except Exception as ex:
        logger.error("[ROW] Insert failed: %s" % ex)


def drop_type_notification(db):
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
    name = 'notify_trigger'
    cmd = "DROP FUNCTION IF EXISTS %s();" % name

    try:
        db.execute_cmd(cmd)
        logger.info("[PROCEDURE] Dropping procedure: %s" % name)
    except Exception as ex:
        logger.error("[PROCEDURE] Dropping procedure failed: %s" % ex)
