from threading import Thread
from etl.monitor import logger


class NotificationThread(Thread):
    conn = None
    new = False

    def __init__(self, conn):
        Thread.__init__(self)
        self.conn = conn

    def run(self):
        self.listen()

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
                db.commit()
                db.poll()
                db.commit()
                while db.notifies:
                    notify = db.notifies.pop()
                    logger.info("""[CORE]
                     New notification: pid=%s channel=%s payload=%s new=%s
                    """ % (
                        notify.pid, notify.channel, notify.payload, self.new))
                    self.new = True
        except Exception as ex:
            logger.error("Details: %s" % ex)
            return
