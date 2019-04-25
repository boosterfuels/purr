from etl.monitor import logger
import pymongo
from bson import ObjectId


def check(db, colls_requested):
    """
    Checks if requested collections exist in the database.
    Gets all collection names from MongoDB (colls_name) and creates
    a new list which contains only the existing collection names.

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
                logger.warn("""
                    [COLLECTION] '%s' is not in the Mongo database. 
                    Skipping data transfer""" % coll)
    except Exception as ex:
        logger.error("[COLLECTION] Checking collection names failed: %s" % ex)
    return colls_existing


def get_by_name(db, name, size=20000):
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
    docs = []
    try:
        logger.info('[COLLECTION] Loading data from collection %s...' % name)
        c = db[name]
        bz = c.find()
        docs = bz.batch_size(size)
    except Exception as ex:
        logger.error(
            """[COLLECTION] Loading data from collection %s failed.
            Details: %s""" % (name, ex))
    return docs


def get_docs_for_type_check(db, name, nr_of_docs=100):
    """
    Gets data from a collection limited.

    Parameters
    ----------
    db : pymongo.database.Database
      Database connection
    name : string
      Name of collection.
    nr_of_docs : integer
      Number of documents to return

    Returns
    -------
    docs : pymongo.cursor.Cursor

    Raises
    ------

    Example
    -------
    get_docs_for_type_check(db, 'Car')

    TODO
    ----
    - let the user decide batch size
    """
    docs = []
    try:
        logger.info('[COLLECTION] Loading data from collection %s...' % name)
        c = db[name]
        docs = c.find().sort(
            '$natural', pymongo.DESCENDING
        ).skip(0).limit(nr_of_docs)
    except Exception as ex:
        logger.error("""
          [COLLECTION] Loading data from collection %s failed.
          Details: %s""" % (name, ex))
    return docs


def get_by_name_reduced(db, name, fields, size=20000):
    """
    Gets data from collection limited by batch size containing
    only specific fields.

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
    docs = []
    try:
        logger.info('[COLLECTION] Loading data from collection %s...' % name)
        c = db[name]
        # create the document given to a query that specifies which
        # fields MongoDB returns in the result set
        projection = {}
        for field in fields:
            projection[field] = 1
        bz = c.find({}, projection).sort('$natural', pymongo.DESCENDING)
        docs = bz.batch_size(size)
    except Exception as ex:
        logger.error(
            """[COLLECTION] Loading data from collection %s failed.
            Details: %s""" % (name, ex))
    return docs


def get_all(db):
    try:
        return db.collection_names(include_system_collections=False)
    except Exception as ex:
        logger.error(
            """[COLLECTION] Loading collection names failed.
            Details:%s""" % (ex))


def get_doc_by_id(db, name, id):
    try:
        c = db[name]
        bz = c.find_one({"_id": ObjectId(id)})
        return bz
    except Exception as ex:
        logger.error(
            """
            [COLLECTION] Loading document from collection %s failed.
            Details: %s""" % (name, ex))
