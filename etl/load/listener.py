import psycopg2
from bson.json_util import loads, dumps

from etl.load import init_pg as pg, table
from etl.monitor import logger
import datetime
from psycopg2.extras import execute_values
import select
# Open a cursor to perform database operations


def listen(pg):
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
    db = pg.conn
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
                raise EnvironmentError
    except Exception as ex:
        logger.error("Details: %s" % ex)
        return
