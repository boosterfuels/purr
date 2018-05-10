import logging

class Logger():
  """Base class for all exceptions."""
  def __init__(self, file, name):
    # create log file handler
    fh = logging.FileHandler(file)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    # add formatter to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    # add the handlers to logger
    self.logger = logging.getLogger(name)
    self.logger.setLevel(logging.DEBUG)

    self.logger.addHandler(ch)
    self.logger.addHandler(fh)

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
