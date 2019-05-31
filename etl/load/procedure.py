from etl.monitor import logger

CURR_FILE = "[PROCEDURE]"


def create_type_notification(db, name):
    """
    Creates a function which will notify channel 'purr' about type changes.

    Parameters
    ----------
    db  : obj
        : Postgres connection object
    name: sting
        : name of the function
    Returns
    -------
    -
    Example
    -------
    create_notification(db, name_function)
    """
    cmd = """CREATE OR REPLACE FUNCTION %s()
    RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('purr', 'type_change');
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    """ % name

    try:
        logger.info("Creating procedure: %s" % name, CURR_FILE)
        db.execute_cmd(cmd)

    except Exception as ex:
        logger.error("Insert failed: %s" % ex, CURR_FILE)


def drop_type_notification(db, name):
    """
    Drops the function which creates a notification.
    The notification created by this function is sent to channel 'purr'
    and signalizes a type change.

    Parameters
    ----------
    db  : obj
        : Postgres connection object
    name: sting
        : name of the function
    Returns
    -------
    -
    Example
    -------
    drop_notification(db, function_name)
    """
    cmd = "DROP FUNCTION IF EXISTS %s();" % name

    try:
        db.execute_cmd(cmd)
        logger.info("Dropping procedure: %s" % name, CURR_FILE)
    except Exception as ex:
        logger.error("Dropping procedure failed: %s" % ex, CURR_FILE)
