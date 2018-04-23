# here comes everything with the oplog
import pprint
import pymongo
import time
from extract import collection, extractor
from load import table, row

from datetime import datetime, timedelta
from bson import Timestamp
  
class Tailer():
  """
  This is a class for extracting data from the oplog.
  TODO
  replace things to base class
  """

  def __init__(self):
    self.tailing = False

  def transform_and_load(self, doc):
    """
    TODO
    - missing transform
    """

    fullname = doc['ns']
    table_name = fullname.split(".")[1]
    row.make_cmd_iud(doc['op'], table_name, doc['o'])
    
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
    Print only write operations    
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
    
    if dt is not None and dt <= now:
      start = dt
    else:
      start = now
    self.tailing = True

    client = pymongo.MongoClient()
    oplog = client.local.oplog.rs

    # Start reading the oplog 
    print('Current time:', now, '\nTailing from:', start, '\nTimestamp:', Timestamp(start, 1))
    latest = oplog.find({'ts': {'$gte': Timestamp(start, 1)}})

    # latest exists
    if (latest.count()):
      latest = latest.next()
      ts_latest = latest['ts'] 
    else:
      print("Empty cursor! Read all...")
      first = oplog.find().sort('$natural', pymongo.ASCENDING).limit(-1).next()
      ts_latest = first['ts']    
    try:

      while True:

        cursor = oplog.find({'ts': {'$gt': ts_latest}},
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
    except StopIteration:
      print("Reading was stopped")

  def stop(self):
    self.tailing = False
    print('Good bye.')
