import logging
import sys


class Logger():
    """Class for logs."""

    def __init__(self, name=""):
        """
          Creates log handlers, adds formatters to them
          and finally adds the handlers to the logger.
        """
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - [%(levelname)s] - %(message)s")

        h = logging.StreamHandler(sys.stdout)
        h.setLevel(logging.DEBUG)
        h.setFormatter(formatter)

        self.logger.addHandler(h)

    def info(self, m):
        self.logger.info(m)

    def warn(self, m):
        self.logger.warning(m)

    def debug(self, m):
        self.logger.debug(m)

    def error(self, m):
        self.logger.error(m)

    def critical(self, m):
        self.logger.critical(m)


logger = Logger()
