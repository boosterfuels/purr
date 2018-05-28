# here comes everything with the oplog
import pymongo
import time
from transform import relation
from extract import extractor

from datetime import datetime, timedelta
from bson import Timestamp
import monitor

INSERT = 'i'
UPDATE = 'u'
DELETE = 'd'

logger = monitor.Logger('oplog-transfer.log', 'TAILER')

class Tailer():
  """
  This is a class for extracting data from the oplog.
  TODO
  replace things to base class
  """

  def __init__(self, pg, schema):
    self.tailing = False
    self.db = pg
    self.schema = schema

  def transform_and_load(self, doc):
    """
    """

    fullname = doc['ns']
    table_name = fullname.split(".")[1]

    oper = doc['op']
    doc_useful = doc['o']

    r = relation.Relation(self.db, self.schema, table_name)
    if r.exists() is False:
      return
      
    if oper == INSERT:
      try:
        print(doc_useful)
        r.insert(doc_useful)
        exit()
      except:
        logger.error("INSERT failed, ObjectId = " + str(doc_useful["_id"]))

    elif oper == UPDATE:
      try:
        r.update(doc_useful)
      except:
        logger.error("UPDATE failed, ObjectId = " + str(doc_useful["_id"]))

    elif oper == DELETE:
      try:
        r.delete(doc_useful)
      except:
        logger.error("DELETE failed, ObjectId = " + str(doc_useful["_id"]))

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

  def start(self, dt):
    """
    Starts tailing the oplog and prints write operation records on command line.
    Parameters
    ----------
    dt: datetime
        If datetime is None, tailing starts from now.
    Not yet :)
    start : timestamp
    Example
    -------
    start_tailing()
    """
    start = None
    now = self.now()
    
    self.tailing = True

    client = pymongo.MongoClient()
    oplog = client.local.oplog.rs

    # Start reading the oplog 
    logger.info(' '.join(['Current time:', str(dt), '\nTailing from:', str(now), '\nTimestamp:', str(Timestamp(dt, 1))]))

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
          time.sleep(1)
    except StopIteration as e:
      logger.error("Tailing was stopped (exc):" + e)

  def stop(self):
    self.tailing = False
    logger.info("Tailing was stopped.")
