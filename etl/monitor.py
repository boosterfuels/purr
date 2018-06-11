import logging
import sys


class Logger():
  """Base class for all exceptions."""
  def __init__(self, file='', name=''):
    # create log file handler
    self.logger = logging.getLogger("logs_purr")
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    ch = logging.StreamHandler()
    # add formatter to the handlers
    formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
    ch.setFormatter(formatter)
    # add the handlers to logger

    self.logger.addHandler(ch)

  def info(self, m):
    self.logger.info(m)

  def warn(self, m):
    self.logger.warn(m)

  def debug(self, m):
    self.logger.debug(m)

  def error(self, m):
    self.logger.error(m)  

  def critical(self, m):
    self.logger.critical(m)

logger = Logger()