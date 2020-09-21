from threading import Thread
from etl.monitor import logger
import time

class NotificationThread(Thread):
    conn = None
    new = False

    def __init__(self, conn):
        Thread.__init__(self)
        self.conn = conn
        self.terminate = False

    def run(self):
        self.listen()

    def stop(self):
        self.terminate = True

    def listen(self):
        """
        Listens for changes from the channel.
        Parameters
        ----------
        db :    pgection
        Returns
        -------
        -
        Example
        -------
        listen(pg)
        """

        channel = 'purr'
        cmd = 'LISTEN %s;' % channel
        db = self.conn.conn
        db.cursor().execute(cmd)
        running = True
        try:
            while running:
                time.sleep(1)
                if self.terminate is True:
                    break
                db.commit()
                db.poll()
                db.commit()
                while db.notifies:
                    notify = db.notifies.pop()
                    logger.info("""[NOTIFICATION]
                     New notification: pid=%s channel=%s payload=%s new=%s
                    """ % (
                        notify.pid, notify.channel, notify.payload, self.new))
                    self.new = True
        except Exception as ex:
            logger.error("[NOTIFICATION] Details: %s" % ex)
