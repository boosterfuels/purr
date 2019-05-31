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

    def info(self, m, location=""):
        self.logger.info(location + " " + m)

    def warn(self, m, location=""):
        self.logger.warning(location + " " + m)

    def debug(self, m, location=""):
        self.logger.debug(location + " " + m)

    def error(self, m, location=""):
        self.logger.error(location + " " + m)

    def critical(self, m, location=""):
        self.logger.critical(location + " " + m)


logger = Logger()
