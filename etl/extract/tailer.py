# here comes everything with the oplog
import pymongo
import time
from datetime import datetime, timedelta
from bson import Timestamp

from etl.transform import relation
from etl.monitor import logger
from etl.extract import extractor

INSERT = 'i'
UPDATE = 'u'
DELETE = 'd'

class Tailer(extractor.Extractor):
  """
  This is a class for extracting data from the oplog.
  TODO
  replace things to base class
  """

  def __init__(self, pg, mdb, setup_pg, settings, coll_settings):
    extractor.Extractor.__init__(self, pg, mdb, setup_pg, settings, coll_settings)
    self.tailing = False

  def transform_and_load(self, doc):
    """
    Gets the document and passes it to the corresponding function in order to exeucte command INSERT/UPDATE/DELETE 
    """
    fullname = doc['ns']
    table_name = fullname.split(".")[1]

    oper = doc['op']
    doc_useful = doc['o']

    doc_id = doc_useful["_id"]
    r = relation.Relation(self.pg, self.schema_name, table_name)
    if r.exists() is False:
      return
      
    if oper == INSERT:
      try:
        if self.typecheck_auto is False:
          super().transfer_doc(doc_useful, r)
        else:
          r.insert(doc_useful)
      except Exception as ex:
        logger.error("INSERT failed, ObjectId = %s. %s %s" % (doc_id, doc_useful, ex))

    elif oper == UPDATE:
      try:
        if self.typecheck_auto is False:
          super().transfer_doc(doc_useful, r)
        else:
          r.update(doc_useful)
      except Exception as ex:
        logger.error("UPDATE failed, ObjectId = %s. %s %s" % (doc_id, doc_useful, ex))
    elif oper == DELETE:
      try:
        r.delete(doc_useful)
      except Exception as ex:
        logger.error("DELETE failed, ObjectId = %s. %s %s" % (doc_id, doc_useful, ex))

  def start_tailing_from_dt(dt):
    """
    Gets timestamp from specific date
    Parameters
    ----------
    dt : date and time
    Returns
    ----------
    start : timestamp
    Example
    -------
    start_tailing_from_dt(2018, 4, 12, 13, 30, 3, 381)
    """
    start = datetime(dt)
    return start

  def now(self):
    """
    Gets timestamp from specific date
    Parameters
    ----------
    None
    Returns
    ----------
    start : timestamp
    Example
    -------
    start_tailing_from_now()
    """
    start = datetime.utcnow()
    start = start - timedelta(minutes=0,
                            seconds=30,
                            microseconds = start.microsecond)
    return start

  def start(self, dt = None):
    """
    Starts tailing the oplog and prints write operation records on command line.
    Parameters
    ----------
    dt: datetime
        If datetime is None, tailing starts from now.
    start : timestamp
    Example
    -------
    start_tailing()
    """
    if dt is None:
      now = self.now()
    else:
      now = dt  
    self.tailing = True

    client = self.mdb.client
    oplog = client.local.oplog.rs

    # Start reading the oplog 
    logger.info('Tailing from: %s: [%s]' % (str(now), str(Timestamp(dt, 1))))
    try:
      while True:
        cursor = oplog.find({'ts': {'$gt': Timestamp(now, 1)}},
            cursor_type = pymongo.CursorType.TAILABLE_AWAIT,
            oplog_replay=True)
      
        while cursor.alive:
          if not self.tailing:
            cursor.next()
          for doc in cursor:
              ts = doc['ts']
              if(doc['op']!='n'):
                self.transform_and_load(doc)
                logger.info(doc)
          time.sleep(1)

    except StopIteration as e:
      logger.error("Tailing was stopped unexpectedly: %s" % e)
    except KeyboardInterrupt:
      logger.error("Tailing was stopped by the user.")
    except Exception as ex:
      logger.error(ex)

  def stop(self):
    self.tailing = False
    logger.info("Tailing was stopped.")
