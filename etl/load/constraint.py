from etl.monitor import logger


def add_pk(db, schema, table, attr):
    """
    Adds primary key to a PostgreSQL table.

    Parameters
    ----------
    db : obj
    schema : string
    table : string
    attr  : string

    Returns
    -------
    -

    Example
    -------
    add_pk(pg, 'public', 'employee', 'id')

    """
    cmd = 'ALTER TABLE %s.%s ADD PRIMARY KEY (%s)' % (
        schema, table.lower(), attr)
    try:
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error(
            """
            [CONSTRAINT] Failed to add primary key to table %s.
            Details: %s
            """ % (table, ex))
