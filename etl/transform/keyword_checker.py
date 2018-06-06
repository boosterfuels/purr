import yaml
import os
from etl.transform.constants import keywords
from etl.monitor import Logger

here = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(here, 'constants/keywords.py')
logger = Logger('collection-transfer.log', 'COLLECTION')

def get_keywords():
  try:
    reserved = [item.split(",")[0] for item in keywords.pg if item.split(",")[1] == 'R']
    return reserved
  except Exception as ex:
    logger.error(ex)
