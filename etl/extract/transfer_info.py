import psycopg2
from datetime import datetime

from etl.load import table, row, constraint, schema
from etl.monitor import logger


def create_stat_table(db, schema):
    """
  Adds primary key to a PostgreSQL table.

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
    values = [datetime.utcnow()]
    try:
        table.create(db, schema, table_name, attrs, types)
        row.insert(db, schema, table_name, attrs, values)
        logger.info("Created table %s." % (table_name))
    except Exception as ex:
        logger.error("[TRANSFER_INFO] Failed to create table purr_info: %s" % (ex))


def get_latest_successful_ts(db, schema):
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
            "[TRANSFER_INFO] Failed to get the timestamp of the latest successful transfer: %s"
            % (ex)
        )


def update_latest_successful_ts(db, schema, dt):
    cmd = "UPDATE %s.purr_info SET latest_successful_ts='%s';" % (schema, str(dt))
    try:
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error(
            "[TRANSFER_INFO] Failed to update the timestamp of the latest successful transfer: %s"
            % (ex)
        )
