import psycopg2
import time

from etl.load import table, row, constraint, schema, procedure
from etl.monitor import logger
from datetime import datetime
import json
import collections
from bson.json_util import ObjectId
import re
from etl.extract import collection
from etl.monitor import logger
from etl.transform import type_checker as tc

import yaml

CURR_FILE = "[COLLECTION_MAP]"


def get_query_update(def_coll, schema, table, attrs, row_id):
    types = []
    for field in def_coll:
        tmp = (str(field).replace("'", '"').replace('None', 'null'))
        types.append("$$%s$$" % tmp)

    query = """
    UPDATE %s.%s SET types = ARRAY[%s]::JSONB[]
    where id=%s;
    """ % (schema, table, ", ".join(types), row_id)

    return query


def populate_coll_map_table(db, coll_map, schema, table, attrs):
    collection_map = collections.OrderedDict(coll_map)
    for coll_name, v in coll_map.items():
        row_id = list(collection_map).index(coll_name)
        query_update = get_query_update(
            v[":columns"], schema, table, attrs, row_id
        )
        values = tuple([row_id,
                        coll_name,
                        v[":meta"][":table"],
                        v[":columns"],
                        datetime.utcnow(),
                        query_update])
        row.upsert_transfer_info(db, schema, table, attrs, values)


def get_coll_map_table(db, schema='public'):
    cmd = """SELECT id, collection_name, relation_name,
    types FROM %s.purr_collection_map ORDER BY id""" % (
        schema)
    try:
        coll_map = db.execute_cmd_with_fetch(cmd)
        logger.info("[TRANSFER_INFO] Getting schema from DB.")
        return coll_map

    except Exception as ex:
        logger.error(
            "[TRANSFER_INFO] Failed to get collection map table"
            % (ex)
        )


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
    attrs = ["id", "collection_name", "relation_name",
             "types", "updated_at", "query_update"]
    types = ["integer", "text", "text", "jsonb[]", "timestamp", "text"]

    try:
        table.drop(db, schema, [table_name])
        table.create(db, schema, table_name, attrs, types)
        logger.info("[TRANSFER INFO] Created table %s.%s." %
                    (schema, table_name))
    except Exception as ex:
        logger.error(
            """
            [TRANSFER_INFO] Failed to create table %s.%s: %s
            """ % (schema, table_name, ex))

    populate_coll_map_table(db, coll_map, schema, table_name, attrs)
    procedure_name = 'notify_type'
    procedure.drop_type_notification(db, procedure_name)
    procedure.create_type_notification(db, procedure_name)
    table.drop_trigger_type_notification(
        db, 'public', 'purr_collection_map', 'notify', procedure_name)
    table.create_trigger_type_notification(
        db, 'public', 'purr_collection_map', 'notify', procedure_name)


def determine_types(mongo, name_db):
    coll_map = {name_db: {}}
    colls = collection.get_all(mongo)

    for coll in sorted(colls):
        logger.info(
            '%s Determining types for collection %s...' %
            (CURR_FILE, coll))

        # TODO: replace snake_case to another file e.g util
        name_relation = tc.snake_case(coll)

        coll_map[name_db][coll] = {
            ":columns": [],
            ':meta': {
                ':table': name_relation,
                ':extra_props': 'JSONB'
            }
        }
        columns = []
        docs = collection.get_docs_for_type_check(mongo, coll)
        types = {}
        logger.info("%s Reading samples..." % (CURR_FILE))

        for doc in docs:
            for k, v in doc.items():
                if k not in types.keys():
                    types[k] = {}
                value_new, type_pg = tc.get_type_pg(v)
                if type_pg in types[k].keys():
                    types[k][type_pg] += 1
                else:
                    types[k][type_pg] = 1

        # TODO: handle None
        for field, value in types.items():
            type_chosen = "text"
            if len(field) > 1:
                sum = docs.count()
                max = 0
                for k, v in value.items():
                    if k is None:
                        continue
                    curr_perc = v/sum
                    if curr_perc > 0 and curr_perc > max:
                        max = curr_perc
                        type_chosen = k
            else:
                # there is exactly one key which will be the
                # chosen type
                type_chosen = list(value.keys())[0]
            name_column = tc.snake_case(field)
            def_column = {
                name_column: None,
                ":source": field,
                ":type": type_chosen.upper()
            }
            coll_map[name_db][coll][":columns"].append(def_column)
    return coll_map


def create_file(coll_map):
    """
    Creates the collection map file.
    """
    name_file = "collections.yml"
    operation = "w"
    try:
        logger.info("%s Creating collection map file..." % CURR_FILE)
        with open(name_file, operation) as file_out:
            yaml.dump(coll_map, file_out, default_flow_style=False)
        logger.info(
            "%s Collection map file created: %s" % (CURR_FILE, name_file))
    except Exception as ex:
        logger.error("%s Failed to create collection map file. Details: %s" %
                     (CURR_FILE, ex))
