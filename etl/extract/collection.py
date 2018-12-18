from etl.monitor import logger
from etl.extract import init_mongo


def check(db, colls_requested):
    """
    Checks if requested collections exist in the database. 
    Gets all collection names from MongoDB (colls_name) and creates a new list which contains only the existing collection names (colls_existing).

    Parameters
    ----------
    db : pymongo.database.Database
      Database connection and name
    colls_requested : list
      Contains the list of requested collection names.

    Returns
    -------
    colls_existing : list
      Contains only existing collection names.

    Raises
    ------
    ValueError
      when a requested collection does not exist in the database (MongoDB)

    Example
    -------
    check(db, ['Car'])  
      []
    check(db, ['Region', 'Customer']
      ['Region', 'Customer']
    """
    colls_name = db.collection_names(include_system_collections=False)
    colls_existing = []
    logger.info('[COLLECTION] Checking collection names...')
    try:
        for coll in colls_requested:
            try:
                colls_name.index(coll)
                colls_existing.append(coll)
            except ValueError:
                logger.error(
                    "[COLLECTION] '%s' is not in the Mongo database." % coll)
    except Exception as ex:
        logger.error("[COLLECTION] Checking collection names failed: %s" % ex)
    return colls_existing


def get_by_name(db, name):
    """
    Gets data from collection limited by batch size.

    Parameters
    ----------
    db : pymongo.database.Database
      Database connection and name
    name : string
      Name of collection.

    Returns
    -------
    docs : pymongo.cursor.Cursor

    Raises
    ------

    Example
    -------
    get_by_name(db, 'Car')

    TODO
    ----
    - let the user decide batch size
    """
    size = 20000
    docs = []
    try:
        logger.info('[COLLECTION] Loading data from collection %s...' % name)
        c = db[name]
        bz = c.find().sort('$natural', pymongo.DESCENDING)
        docs = bz.batch_size(size)
    except Exception as ex:
        logger.error(
            '[COLLECTION] Loading data from collection %s failed.' % name)
    return docs


def get_by_name_reduced(db, name, fields):
    """
    Gets data from collection limited by batch size containing only specific fields.

    Parameters
    ----------
    db : pymongo.database.Database
      Database connection and name
    name : string
      Name of collection.
    fields : list
      Names of fields to include in the query.

    Returns
    -------
    docs : pymongo.cursor.Cursor

    Raises
    ------

    Example
    -------
    get_by_name(db, 'Car', ['_id', 'type', 'nfOfSeats'])

    TODO
    ----
    - let the user decide batch size
    """
    size = 20000
    docs = []
    try:
        logger.info('[COLLECTION] Loading data from collection %s...' % name)
        c = db[name]
        # create the document given to a query that specifies which fields MongoDB returns in the result set
        projection = {}
        for field in fields:
            projection[field] = 1
        bz = c.find({}, projection)
        docs = bz.batch_size(size)
    except Exception as ex:
        logger.error(
            '[COLLECTION] Loading data from collection %s failed.' % name)
    return docs
