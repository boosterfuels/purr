import yaml
import os
from etl.transform.constants import keywords
from etl.monitor import logger

here = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(here, 'constants/keywords.py')

def get_keywords():
  try:
    reserved = [item.split(",")[0] for item in keywords.pg if item.split(",")[1] == 'R']
    return reserved
  except Exception as ex:
    logger.error("[COLLECTION] %s" % ex)
