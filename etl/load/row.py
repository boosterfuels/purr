import psycopg2
from bson.json_util import dumps

from etl.load import init_pg as pg, table
from etl.monitor import logger
import datetime
from psycopg2.extras import execute_values
import json

# Open a cursor to perform database operations


def insert(db, schema, table, attrs, values):
    """
    Inserts a row defined by attributes and values into a specific
    table of the PG database.

    Parameters
    ----------
    table_name : string
    attrs :     string[]
    values :    string[]

    Returns
    -------
    -

    Example
    -------
    insert('Audience', [attributes], [values])
    """
    temp = []
    for v in values:
        if type(v) is list:
            if type(v[0]) is str and v[0].startswith("{"):
                temp.append('array[%s]::jsonb[]')
                continue
            temp.append('%s')

        else:
            temp.append('%s')

    temp = ', '.join(temp)
    attrs = ', '.join(attrs)

    cmd = "INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT DO NOTHING;" % (
        schema, table.lower(), attrs, temp)

    # MoSQL ignores the document and logs a warning
    # if a document could not be inserted.
    # We will decide later what to do with DataErrors.
    try:
        db.execute_cmd(cmd, values)
    except Exception as ex:
        logger.error("[ROW] Insert failed: %s", ex)


def insert_bulk(db, schema, table, attrs, values):
    """
    Inserts a row defined by attributes and values into a specific
    table of the PG database.

    Parameters
    ----------
    table_name : string
    attrs :     string[]
    values :    string[]

    Returns
    -------
    -

    Example
    -------
    insert('Audience', [attributes], [values])
    """
    # prepare attributes for insert
    attrs_reduced = ', '.join([('"%s"' % a) for a in attrs])
    attrs = ', '.join([('"%s"' % a) for a in attrs])

    # default primary key in Postgres is name_of_table_pkey
    constraint = '%s_pkey' % table
    cmd = "INSERT INTO %s.%s (%s) VALUES %s;" % (
        schema, table.lower(), attrs, '%s')
    try:
        execute_values(db.cur, cmd, values)
        db.conn.commit()
    except Exception as ex:
        logger.error("[ROW] INSERT failed: %s" % ex)
        logger.error("[ROW] CMD: %s" % cmd)
        logger.error("[ROW] VALUES: %s" % values)
        raise SystemExit()


def upsert_bulk(db, schema, table, attrs, rows):
    """
    Inserts a row defined by attributes and values into a specific
    table of the PG database.

    Parameters
    ----------
    db : obj
    schema : string
    table_name : string
    attrs :     string[]
    rows :    string[]

    Returns
    -------
    -

    Example
    -------
    upsert_bulk(db, 'public', 'audience', [attributes], [values])
    Note: command is different for Postgres v10+:
    cmd = "INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT ON CONSTRAINT %s
    DO UPDATE SET (%s) = ROW(%s);"
    """
    temp = []
    for v in rows[0]:
        if type(v) is str and v.startswith("[{"):
            temp.append('array[%s]::jsonb[]')
        else:
            temp.append('%s')

    temp = ', '.join(temp)

    attrs_reduced = [('"%s"' % a) for a in attrs]
    attrs_reduced = ', '.join(attrs_reduced)

    excluded = [('EXCLUDED.%s' % a) for a in attrs]
    excluded = ', '.join(excluded)

    attrs = [('"%s"' % a) for a in attrs]
    attrs = ', '.join(attrs)

    # default primary key in Postgres is name_of_table_pkey
    constraint = '%s_pkey' % table

    cmd = """
    INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT ON CONSTRAINT %s
    DO UPDATE SET (%s) = ROW(%s);
    """ % (
        schema, table.lower(), attrs, temp,
        constraint, attrs_reduced, excluded)
    try:
        db.cur.executemany(cmd, rows)
        db.conn.commit()
    except Exception as ex:
        logger.error("[ROW] UPSERT failed: %s" % ex)
        logger.error("[ROW] CMD:\n %s" % cmd)
        logger.error("[ROW] VALUES:\n %s" % rows)


def upsert_bulk_tail(db, schema, table, attrs, rows):
    """
    Inserts a row defined by attributes and values into a specific
    table of the PG database.

    Parameters
    ----------
    table_name : string
    attrs :     string[]
    values :    string[]

    Returns
    -------
    -

    Example
    -------
    insert('Audience', [attributes], [values])
    """
    try:
        for i in range(0, len(rows)):
            row = rows[i]
            values = []
            attrs_reduced = []
            for j in range(0, len(attrs)):
                if row[j] == 'unset':
                    values.append(None)
                    attrs_reduced.append(attrs[j])
                elif row[j] is not None:
                    values.append(row[j])
                    attrs_reduced.append(attrs[j])
            upsert_bulk(db, schema, table, attrs_reduced, [tuple(values)])
        db.conn.commit()

    except Exception as ex:
        logger.error("[ROW] UPSERT failed when tailing: %s" % ex)
        logger.error("[ROW] VALUES: %s" % values)
        raise SystemExit()


def upsert_transfer_info(db, schema, table, attrs, row):
    """
    Updates the collection map.
    """
    temp = []
    temp_row = []
    for r in row:
        if type(r) is list and type(r[0]) is dict:
            temp.append('%s::jsonb[]')
        else:
            temp.append('%s')

    placeholder = []

    values = []
    for r in row:
        if type(r) is list and type(r[0]) is dict:
            for item in r:
                temp_row.append(json.dumps(item))
            placeholder.append("%s::jsonb[]")
        else:
            placeholder.append("'%s'" % r)

    placeholder = ','.join(placeholder)

    temp = ', '.join(temp)

    attrs_reduced = [('"%s"' % a) for a in attrs]
    attrs_reduced = ', '.join(attrs_reduced)

    excluded = [('EXCLUDED.%s' % a) for a in attrs]
    excluded = ', '.join(excluded)

    attrs = [('"%s"' % a) for a in attrs]
    attrs = ', '.join(attrs)

    # default primary key in Postgres is name_of_table_pkey
    constraint = '%s_pkey' % table

    # upsert
    cmd = """INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT ON CONSTRAINT %s
    DO UPDATE SET (%s) = (%s);""" % (
        schema, table.lower(), attrs, placeholder,
        constraint, attrs_reduced, excluded)

    try:
        db.cur.execute(cmd, [temp_row])
        db.conn.commit()
    except Exception as ex:
        logger.error("[ROW] UPSERT TRANSFER INFO failed: %s" % ex)
        logger.error("\n[ROW] CMD:\n %s" % cmd)
        logger.error("\n[ROW] VALUES:\n %s" % row)
        raise SystemExit()


def update(db, schema, table_name, attrs, values):
    """
    Updates a row in a specific table of the PG database.

    Parameters
    ----------
    table_name : string
    attrs :     string[]
    values :    string[]

    Returns
    -------
    -

    Example
    -------
    update('audience', [attributes], [values])

    """
    attr_val_pairs = []

    oid = ""
    nr_of_attrs = len(attrs)

    if nr_of_attrs < 2:
        return
    for i in range(len(attrs)):
        pair = ""
        if attrs[i] == "id":
            oid = "'%s'" % str(values[i])
            continue
        if values[i] is None:
            pair = "%s = null" % (attrs[i])
        elif type(values[i]) is datetime.datetime:
            pair = "%s = '%s'" % (attrs[i], values[i])
        elif type(values[i]) is str:
            if values[i].startswith("{") is True:
                pair = "%s = '%s'" % (attrs[i], values[i])
            pair = "%s = '%s'" % (attrs[i], values[i])
        else:
            pair = "%s = %s" % (attrs[i], values[i])
        attr_val_pairs.append(pair)

    pairs = ", ".join(attr_val_pairs)
    cmd = "UPDATE %s.%s SET %s WHERE id = %s;" % (
        schema, table_name.lower(), pairs, oid)
    logger.info("[ROW] %s" % cmd)
    try:
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error('[ROW] Update failed: %s' % ex)


def delete(db, schema, table_name, ids):
    """
    Deletes a row in a specific table of the PG database.

    Parameters
    ----------
    table_name : string
    object_id : ObjectId
                (will need to get the hex encoded version
                of ObjectId with str(object_id))

    Returns
    -------
    -

    Example
    -------
    delete(db, schema, 'Audience', ObjectId("5acf593eed101e0c1266e32b"))

    """
    oids = "','".join(ids)
    cmd = "DELETE FROM %s.%s WHERE id IN ('%s');" % (
        schema, table_name.lower(), oids)
    logger.info("[ROW] %s" % cmd)
    try:
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error("[ROW] Delete failed: %s" % ex)
