import psycopg2
import psycopg2.extras
import time
from etl.monitor import logger
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

CURR_FILE = "[INIT_PG]"


class PgConnection:

    def __init__(self, conn_details, ttw=1):
        logger.info("%s Connecting to %s" % (CURR_FILE, conn_details))

        # time to wait before attempt to reconnect
        self.ttw = ttw
        self.conn_details = conn_details
        if ttw == 1:
            self.attempt_to_reconnect = False
        try:
            self.conn = psycopg2.connect(self.conn_details)
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            self.cur = self.conn.cursor()
            logger.info("%s Connected to Postgres." % (CURR_FILE))
            self.ttw = 1
        except Exception as ex:
            self.attempt_to_reconnect = True
            logger.error(
                """
                %s Could not connect to Postgres.
                Reconnecting in %s seconds...
                Details: %s
                """
                % (CURR_FILE, self.ttw, ex)
            )
            time.sleep(self.ttw)

            self.__init__(self.conn_details, self.ttw * 2)

    def execute_cmd(self, cmd, values=None):
        cur = self.conn.cursor()
        try:
            if values is not None:
                cur.execute(cmd, values)
            else:
                cur.execute(cmd)

        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            self.handle_interface_and_oper_error()
        except Exception as ex:
            logger.error(
                """
                %s Executing query without fetch failed.
                Details: %s
                """ % (CURR_FILE, ex))
        cur.close()

    def execute_cmd_with_fetch(self, cmd, values=None):
        cur = self.conn.cursor()

        try:
            if values is not None:
                cur.execute(cmd, values)
            else:
                cur.execute(cmd)
            return cur.fetchall()

        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            self.handle_interface_and_oper_error()
        except Exception as ex:
            logger.error(
                """
                %s Executing query with fetch failed.
                Details: %s
                """ % (CURR_FILE, ex))
        cur.close()

    def execute_many_cmd(self, cmd, values):
        cur = self.conn.cursor()
        try:
            cur.executemany(cmd, values)
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            handle_interface_and_oper_error()
        except Exception as ex:
            logger.error(
                """
                %s Executing many failed.
                Details: %s
                """ % (CURR_FILE, ex))
        cur.close()

    def poll(self):
        cur = self.conn.cursor()
        cur.execute()
        cur.close()

    def notifies(self):
        return self.conn.notifies

    def __del__(self):
        self.conn.close()
        self.cur.close()

    def handle_interface_and_oper_error(self):
        logger.error("%s Trying to reconnect to Postgres..." % (CURR_FILE))
        self.attempt_to_reconnect = True
        self.__init__(self.conn_details, self.ttw * 2)
