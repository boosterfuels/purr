import psycopg2
import time

from etl.load import table, row, constraint, schema, procedure
from etl.monitor import logger
from datetime import datetime
import json
import collections


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
    values = [int(time.time())]
    try:
        table.create(db, schema, table_name, attrs, types)
        ts = get_latest_successful_ts(db, 'public')
        if len(ts) == 0:
            row.insert(db, schema, table_name, attrs, values)
        logger.info("[TRANSFER INFO] Created table %s." % (table_name))
    except Exception as ex:
        logger.error(
            "[TRANSFER_INFO] Failed to create table %s: %s" % (table_name, ex))


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
    cmd = "UPDATE %s.purr_info SET latest_successful_ts='%s';" % (
        schema, str(dt))
    try:
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error(
            "[TRANSFER_INFO] Failed to update the timestamp of the latest successful transfer: %s"
            % (ex)
        )


def populate_coll_map_table(db, coll_map, schema, table, attrs):
    # def upsert_bulk(db, schema, table, attrs, values):
    collection_map = collections.OrderedDict(coll_map)
    values = []
    for coll_name, v in coll_map.items():
        columns = json.dumps(v[":columns"])
        values.append(
            tuple([list(collection_map).index(coll_name), coll_name, v[":meta"][":table"], columns, datetime.utcnow()]))
    row.upsert_bulk(db, schema, table, attrs, values)


def create_coll_map_table(db, schema, coll_map):
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
    table_name = "purr_collection_map"
    attrs = ["id", "collection_name", "relation_name", "types", "updated_at"]
    types = ["integer", "text", "text", "jsonb[]", "timestamp"]
    values = [int(time.time())]

    try:
        table.drop(db, schema, [table_name])
        table.create(db, schema, table_name, attrs, types)
        logger.info("[TRANSFER INFO] Created table %s.%s." %
                    (schema, table_name))
    except Exception as ex:
        logger.error(
            "[TRANSFER_INFO] Failed to create table %s.%s: %s" % (schema, table_name, ex))

    populate_coll_map_table(db, coll_map, schema, table_name, attrs)
    procedure_name = 'notify_type'
    procedure.drop_type_notification(db, procedure_name)
    procedure.create_type_notification(db, procedure_name)
    table.drop_trigger_type_notification(
        db, 'public', 'purr_collection_map', 'notify', procedure_name)
    table.create_trigger_type_notification(
        db, 'public', 'purr_collection_map', 'notify', procedure_name)
