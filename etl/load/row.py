from etl.monitor import logger
from psycopg2.extras import execute_values
import json


def insert(db, schema, table, attrs, values):
    """
    Inserts a row into a specific table of the PG database.

    Parameters
    ----------
    db : obj
       : database connection object
    schema : string
           : name of schema
    table : string
          : name of table
    attrs : string[]
          : attributes (column names)
    values : string[]
           : values to insert; its length must be equal to len(attrs)
    Returns
    -------
    -

    Example
    -------
    insert('pg', 'public', 'company', attributes, values)
    TODO: schema should default to public
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
    db.execute_cmd(cmd, values)


def insert_bulk(db, schema, table, attrs, values):
    """
    Inserts multiple rows into a specific table of the PG database.

    Parameters
    ----------
    db : obj
       : database connection object
    schema : string
           : name of schema
    table : string
          : name of table
    attrs : string[]
          : attributes (column names)
    values : string[][]
           : values to insert
    Returns
    -------
    -

    Example
    -------
    insert('pg', 'public', 'company', attributes, values)
    TODO: schema should default to public
    """
    # prepare attributes for insert
    attrs = ', '.join([('"%s"' % a) for a in attrs])

    # default primary key in Postgres is name_of_table_pkey
    cmd = "INSERT INTO %s.%s (%s) VALUES %s;" % (
        schema, table.lower(), attrs, '%s')
    try:
        cur = db.conn.cursor()
        execute_values(cur, cmd, values)
        cur.close()
    except Exception as ex:
        logger.error("[ROW] INSERT bulk failed: %s" % ex)
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
    upsert_bulk(db, 'public', 'employee', [attributes], [values])
    Note: command is different for Postgres v10+:
    cmd = "INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT ON CONSTRAINT %s
    DO UPDATE SET (%s) = ROW(%s);"
    """
    NR_OF_ROWS_TO_DISPLAY = 20
    temp = []
    for v in rows[0]:
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
    DO UPDATE SET (%s) = (%s);
    """ % (
        schema, table.lower(), attrs, temp,
        constraint, attrs_reduced, excluded)
    db.execute_many_cmd(cmd, rows)


def upsert_bulk_tail(db, schema, table, attrs, rows):
    """
    Inserts a row defined by attributes and values into a specific
    table of the PG database.
    Checks for values that are unset during tailing.

    Parameters
    ----------
    db : object
    schema : string
    table : string
    attrs : string[]
    rows : string[]

    Returns
    -------
    -

    Example
    -------
    upsert_bulk_tail(pg, 'public', 'employee', [attributes], [values])
    """
    for i in range(0, len(rows)):
        row = rows[i]
        values = []
        attrs_reduced = []
        for j in range(0, len(attrs)):
            if row[j] == '$unset':
                values.append(None)
                attrs_reduced.append(attrs[j])
            elif row[j] is not None:
                values.append(row[j])
                attrs_reduced.append(attrs[j])
        upsert_bulk(db, schema, table, attrs_reduced, [tuple(values)])




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
        cur = db.conn.cursor()
        cur.execute(cmd, [temp_row])
    except Exception as ex:
        logger.error("[ROW] UPSERT TRANSFER INFO failed: %s" % ex)
        logger.error("\n[ROW] CMD:\n %s" % cmd)
        logger.error("\n[ROW] VALUES:\n %s" % row)
        raise SystemExit()
    cur.close()


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
    delete(db, 'public', 'employee', "5acf593eed101e0c1266e32b")

    """
    oids = "','".join(ids)
    cmd = "DELETE FROM %s.%s WHERE id IN ('%s');" % (
        schema, table_name.lower(), oids)
    logger.info("[ROW] %s" % cmd)
    db.execute_cmd(cmd)
