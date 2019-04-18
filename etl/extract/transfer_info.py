import time

from etl.load import table, row
from etl.monitor import logger


def create_stat_table(db, schema='public'):
    """
    Creates a table that holds the timestamp of the
    latest successfully inserted item.
    Parameters
    ----------

    Returns
    -------
    -

    Example
    -------
    create_stat_table(pg, 'purr')

    """
    table_name = "purr_info"
    attrs = ["latest_successful_ts"]
    types = ["TEXT"]
    values = [int(time.time())]
    try:
        table.create(db, schema, table_name, attrs, types)
        ts = get_latest_successful_ts(db, schema)
        if len(ts) == 0:
            row.insert(db, schema, table_name, attrs, values)
        logger.info("[TRANSFER INFO] Created table %s." % (table_name))
    except Exception as ex:
        logger.error(
            "[TRANSFER_INFO] Failed to create table %s: %s" % (table_name, ex))


def get_latest_successful_ts(db, schema='public'):
    """
  Get the timestamp of the latest successful transfer.

  Parameters
  ----------

  Returns
  -------
  -

  Example
  -------
  get_latest_successful_ts(pg, 'purr')

  """
    cmd = "SELECT latest_successful_ts FROM %s.purr_info;" % (schema)

    try:
        res = db.execute_cmd_with_fetch(cmd)
        return res
    except Exception as ex:
        logger.error(
            """[TRANSFER_INFO] Failed to get the timestamp
             of the latest successful transfer: %s"""
            % (ex)
        )


def update_latest_successful_ts(db, schema, dt):
    cmd = "UPDATE %s.purr_info SET latest_successful_ts='%s';" % (
        schema, str(dt))
    try:
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error(
            """[TRANSFER_INFO] Failed to update the timestamp
            of the latest successful transfer: %s"""
            % (ex)
        )


def create_log_table(db, schema='public'):
    """
    Logs the operation, relation name, object id and
    timestamp for each entry of the oplog.

    Parameters
    ----------
    db: connection obj
    schema: name of the schema in Postgres
    Returns
    -------
    -

    Example
    -------
    create_log_table(pg, 'purr')

    """
    table_name = "purr_log"
    attrs = ["operation", "relation", "id", "ts"]
    types = ["TEXT", "TEXT", "TEXT", "INTEGER"]
    values = [int(time.time())]
    try:
        table.create(db, schema, table_name, attrs, types)
        logger.info("[TRANSFER INFO] Created table %s." % (table_name))
    except Exception as ex:
        logger.error(
            "[TRANSFER_INFO] Failed to create table %s: %s" % (table_name, ex))


def log_rows(db, schema, values):
    """
    Holds the operation, relation name, object id and
    timestamp for each entry of the oplog.

    Parameters
    ----------

    Returns
    -------
    -

    Example
    -------
    create_log_table(pg, 'purr')

    """
    table_name = "purr_log"
    attrs = ["operation", "relation", "id", "ts"]
    try:
        row.insert_bulk(db, schema, table_name, attrs, values)
    except Exception as ex:
        logger.error(
            "[TRANSFER_INFO] Failed to insert logs into table %s: %s"
            % (table_name, ex))
