# access MongoDB
import pymongo
from etl.monitor import logger


class MongoConnection():
    """
    TODO
    - create a base class for Connection
    - put dbname somewhere else
    """

    def __init__(self, settings):
        db_name = settings['db_name']
        try:
            self.client = pymongo.MongoClient(settings['connection'])
        except Exception as ex:
            logger.error("Could not initialize MongoDB client: %s" % ex)
        try:
            self.conn = self.client[db_name]
        except Exception as ex:
            logger.error("Could not create connection to MongoDB: %s" % ex)

    def disconnect(self):
        logger.info("MongoDB says goodbye.")
        self.client.close()
