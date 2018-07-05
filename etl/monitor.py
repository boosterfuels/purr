import logging
import sys


class Logger():
  """Base class for all exceptions."""
  def __init__(self, file='', name=''):
    '''
    Creates log handlers, adds formatters to them and finally adds the handlers to the logger
    '''
    # 
    self.logger = logging.getLogger('logs_purr')
    self.logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')

    h1 = logging.StreamHandler(sys.stdout)
    h1.setLevel(logging.INFO)
    h1.setFormatter(formatter)

    h2 = logging.StreamHandler(sys.stderr)
    h2.setLevel(logging.ERROR)
    h2.setFormatter(formatter)

    self.logger.addHandler(h1)
    self.logger.addHandler(h2)

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