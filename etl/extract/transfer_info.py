import time

from etl.load import table, row
from etl.monitor import logger

table_desc = {
    "purr_oplog": {
        "attrs": ["id", "operation", "relation", "obj_id", "ts", "merged", "document"],
        "types": ["SERIAL", "TEXT", "TEXT", "TEXT", "INTEGER", "BOOLEAN", "TEXT"],
        "pks": ["id", "ts"]
    },
    "purr_info": {
        "attrs": ["id", "relation", "latest_successful_ts"],
        "types": ["INTEGER", "TEXT", "TEXT"]
    }
}


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
    attrs = table_desc[table_name]["attrs"]
    types = table_desc[table_name]["types"]
    values = [0, None, int(time.time())]
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
    table_name = 'purr_info'
    cmd = "SELECT latest_successful_ts FROM %s.%s;" % (schema, table_name)

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
    table_name = "purr_oplog"
    attrs = table_desc[table_name]["attrs"]
    types = table_desc[table_name]["types"]
    pks = table_desc[table_name]["pks"]

    values = [int(time.time())]
    try:
        table.drop(db, schema, [table_name])
        table.create(db, schema, table_name, attrs, types, pks)
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
    table_name = "purr_oplog"
    # id is SERIAL type, we can skip it when inserting rows:
    attrs = table_desc[table_name]["attrs"][1:]
    try:
        row.insert_bulk(db, schema, table_name, attrs, values)
    except Exception as ex:
        logger.error(
            "[TRANSFER_INFO] Failed to insert logs into table %s: %s"
            % (table_name, ex))
