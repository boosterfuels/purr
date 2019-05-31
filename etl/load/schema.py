from etl.monitor import logger


def reset(db, schema='public'):
    """
    Reset existing schema or create a new one.
    """
    drop = 'DROP SCHEMA IF EXISTS %s CASCADE;' % schema
    create = 'CREATE SCHEMA %s;' % schema
    try:
        db.execute_cmd(drop)
        db.execute_cmd(create)
        logger.info("[SCHEMA] Schema %s is reset." % schema)
    except Exception as ex:
        logger.error("[SCHEMA] Schema reset failed. %s" % ex)


def create(db, schema='public'):
    """
    Create schema if it does not exist.
    """
    cmd = 'CREATE SCHEMA IF NOT EXISTS %s;' % (schema)
    try:
        db.execute_cmd(cmd)
    except Exception as ex:
        logger.error("""
        [SCHEMA] Creating schema with name %s failed.
        Details: %s
        """ % (schema, ex))
