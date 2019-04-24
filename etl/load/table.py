import psycopg2
from etl.monitor import logger


def create(db, schema, name, attrs, types, pks=["id"]):
    """
    Creates a table in Postgres.
    Parameters
    ----------
    name : str
    TODO
    ----
    """
    attrs_and_types = []

    for i in range(len(attrs)):
        pair = '"%s" %s' % (attrs[i], types[i])
        attrs_and_types.append(pair)

    pks = [('"%s"' % pk) for pk in pks]
    primary_keys = "PRIMARY KEY (%s)" % ",".join(pks)
    attrs_and_types.append(primary_keys)
    attrs_and_types = ", ".join(attrs_and_types)

    name = name.lower()
    cmd = "CREATE TABLE IF NOT EXISTS %s.%s(%s);" % (
        schema, name, attrs_and_types)
    try:
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))


def exists(db, schema, table):
    """
    Check if a table exists in the PG database.

    Parameters
    ----------
    table : string

    Returns
    -------
    True: table exists in the database
    False: otherwise
    """
    cmd = """
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='%s' AND table_name='%s';
    """ % (
        schema, table.lower())

    try:
        res = db.execute_cmd_with_fetch(cmd)
        if res:
            return True
        else:
            return False
    except Exception as ex:
        logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))


def truncate(db, schema, tables):
    """
    Parameters
    ----------
    tables: string[]
    Deletes data from 1..* tables.
    TODO
    ----
    Check if table exists before doing anything.
    """
    tables_cmd = []
    for t in tables:
        tables_cmd.append('%s.%s' % (schema, t))
    tables_cmd = ','.join(tables_cmd)
    cmd = "TRUNCATE TABLE %s;" % (tables_cmd)

    try:
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))


def drop(db, schema, tables):
    """
    Drop one or more tables in the PG database.

    Parameters
    ----------
    schema : string
    tables : list

    Example
    -------
    drop(pg, 'public', ['my_table'])

    Todo
    ----
    - first check if all tables in the list exist
    """
    tables_cmd = []
    for t in tables:
        tables_cmd.append('%s.%s' % (schema, t.lower()))
    tables_cmd = ', '.join(tables_cmd)

    cmd = "DROP TABLE IF EXISTS %s" % (tables_cmd)
    try:
        db.execute_cmd(cmd)
        logger.info('[TABLE] Dropping table(s) %s.' % (tables_cmd))
    except Exception as ex:
        logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))


def add_column(db, schema, table, column_name, column_type):
    """
    Add new column to a specific table.
    Parameters
    ----------
    table : str
          : name of table to alter
    column_name : str
                : name of new column
    column_type : str
                : type of new column
    Example
    -------
    add_column(db, 'some_integer', 'integer')
    """
    cmd = "ALTER TABLE IF EXISTS %s.%s ADD COLUMN IF NOT EXISTS %s %s;" % (
        schema, table.lower(), column_name, column_type)
    logger.warn("""
    [TABLE] Adding new column to table: %s, column: %s, type: %s
    """ % (
        table.lower(), column_name, column_type))
    try:
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))


def add_multiple_columns(db, schema, table, attrs, types):
    """
    Add new column to a specific table.
    Parameters
    ----------
    name : str
    column_name : str
    column_type : str

    Example
    -------
    add_multiple_columns(db, ['nyanya', some_integer'], ['text', integer'])
    """
    statements_add = []
    attrs_types = zip(attrs, types)
    for i, j in attrs_types:
        statements_add.append(' '.join(['ADD COLUMN IF NOT EXISTS', i, j]))
    statements_merged = ', '.join(statements_add)

    cmd = "ALTER TABLE IF EXISTS %s.%s %s;" % (
        schema, table.lower(), statements_merged)

    for i, j in zip(attrs, types):
        logger.warn("Adding column %s (%s) to company %s." %
                    (i, j, table.lower()))
    try:
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))


def column_change_type(db, schema, table, column_name, column_type):
    """
    Add new column to a specific table.
    Parameters
    ----------
    name : str
    column_name : str
    column_type : str

    Example
    -------
    column_change_type(pg.db, 'some_integer', 'integer')
    """
    expression = ''
    if column_type == 'jsonb':
        expression = 'to_json(%s)' % column_name
    elif column_type == 'double precision':
        expression = 'CAST(%s as double precision)' % column_name

    if len(expression) == 0:
        cmd = "ALTER TABLE %s.%s ALTER COLUMN %s TYPE %s;" % (
            schema, table.lower(), column_name, column_type)
    else:
        cmd = "ALTER TABLE %s.%s ALTER COLUMN %s TYPE %s USING %s;" % (
            schema, table.lower(), column_name, column_type, expression)
    logger.warn("""
    [TABLE] ALTER TABLE %s, changing type of column '%s' to '%s'
    """ % (
        table.lower(), column_name, column_type))

    try:
        db.execute_cmd(cmd)
    except psycopg2.ProgrammingError as ex:
        logger.error(
            """
            [TABLE] ProgrammingError: %s when executing command %s.
            """ % (ex, cmd))
    except Exception as ex:
        logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))


def remove_column(db, schema, table, column_name):
    """
    Remove a column from a table.
    Parameters
    ----------
    db: obj
    schema: str
    table: str
    column_name: str

    Example
    -------
    remove_column(pg, 'public', 'user', age')
    """
    cmd = "ALTER TABLE IF EXISTS %s.%s DROP COLUMN IF EXISTS %s;" % (
        schema, table.lower(), column_name)
    try:
        logger.warn("""
        [TABLE] Removing column '%s' from table '%s.%s' if exists.
        """ % (
            column_name, schema, table.lower()))
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))


def get_column_names_and_types(db, schema, table):
    """
    Get column names and column types of a specific table.
    Parameters
    ----------
    table_name: str
    Returns
    -------
    List of column names and corresponding types.
    """
    cmd = """
    SELECT column_name, data_type FROM information_schema.columns
    WHERE table_schema='%s' AND table_name = '%s';
    """ % (
        schema, table.lower())
    logger.info("[TABLE] Checking columns and types for table %s.%s" %
                (schema, table))
    try:
        rows = db.execute_cmd_with_fetch(cmd)
        return rows
    except Exception as ex:
        logger.error('[TABLE] %s when executing command %s.' % (ex, cmd))


def create_trigger_type_notification(db, schema, table, name, proc):
    cmd = """
    CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s.%s
    FOR EACH ROW EXECUTE PROCEDURE %s()
    """ % (name, schema, table, proc)
    try:
        logger.info("[TABLE] Creating trigger '%s'" % name)
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error("[TABLE] Creating trigger '%s' failed: %s" % (name, ex))


def drop_trigger_type_notification(db, schema, table, name, proc):
    cmd = "DROP TRIGGER IF EXISTS %s ON %s.%s CASCADE" % (name, schema, table)
    try:
        logger.info("[TABLE] Dropping trigger '%s'" % name)
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error("[TABLE] Dropping trigger '%s' failed: %s" % (name, ex))

def vacuum(db, schema, table):
    cmd = "VACUUM FULL ANALYZE %s.%s;" % (schema, table)
    try:
        logger.info("[TABLE] Vacuuming table '%s.%s'" % (schema, table))
        db.execute_cmd(cmd)    
    except Exception as ex:
        logger.error("[TABLE] Vacuuming table '%s.%s' failed: %s" % (schema, table))

