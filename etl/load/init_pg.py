import psycopg2
import psycopg2.extras
import time
from etl.monitor import logger


class PgConnection:

    def __init__(self, conn_details, ttw=1):
        logger.info("[INIT_PG] Connecting to %s" % conn_details)

        # time to wait before attempt to reconnect
        self.ttw = ttw
        self.conn_details = conn_details
        if ttw == 1:
            self.attempt_to_reconnect = False
        try:
            self.conn = psycopg2.connect(self.conn_details)
            self.cur = self.conn.cursor()
            logger.info("[INIT_PG] Connected to Postgres.")
            self.ttw = 1
        except Exception as ex:
            self.attempt_to_reconnect = True
            logger.error(
                "[INIT_PG] Could not connect to Postgres. Reconnecting in %s seconds..."
                % self.ttw
            )
            time.sleep(self.ttw)

            self.__init__(self.conn_details, self.ttw * 2)

    def get_conn(self):
        return self.conn

    def get_cur(self):
        return self.cur

    def execute_cmd(self, cmd, values=None):
        try:
            if values is not None:
                self.cur.execute(cmd, values)
            else:
                self.cur.execute(cmd)
                self.conn.commit()

        except (psycopg2.InterfaceError, psycopg2.OperationalError) as exc:
            self.handle_interface_and_oper_error()
        except Exception as ex:
            logger.info(
                "[INIT_PG] Executing query without fetch failed. Details: %s" % ex)
            print(cmd)

    def execute_cmd_with_fetch(self, cmd, values=None):
        try:
            if values is not None:
                self.cur.execute(cmd, values)
            else:
                self.cur.execute(cmd)
                self.conn.commit()
            return self.cur.fetchall()

        except (psycopg2.InterfaceError, psycopg2.OperationalError) as exc:
            self.handle_interface_and_oper_error()
        except Exception as ex:
            logger.info(
                "[INIT_PG] Executing query with fetch failed. Details: %s" % ex)

    def poll(self):
        self.cur.execute(cmd)
        self.cur.commit()
    
    def notifies(self):
        return self.conn.notifies

    def __del__(self):
        self.conn.close()
        self.cur.close()

    def handle_interface_and_oper_error(self):
        logger.error(
            "[INIT_PG] Executing query failed. MOGRIFIED: %s" % self.cur.mogrify(cmd))
        logger.error("[INIT_PG] Trying to reconnect to Postgres...")
        self.attempt_to_reconnect = True
        self.__init__(self.conn_details, self.ttw * 2)
