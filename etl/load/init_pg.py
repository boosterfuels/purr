import psycopg2
import psycopg2.extras
import time
from etl.monitor import logger


class PgConnection:
    """
  TODO
  create a base class for Connection
  """

    def __init__(self, conn_details, ttw=1):
        # time to wait before attempt to reconnect
        self.ttw = ttw
        self.conn_details = conn_details
        if ttw == 1:
            self.attempt_to_reconnect = False
        settings_local = ["db_name", "user"]
        settings_remote = ["db_name", "user", "password", "host", "port"]
        if set(settings_remote).issubset(conn_details):
            self.props = "dbname=%s user=%s password=%s host=%s port=%s" % (
                conn_details["db_name"],
                conn_details["user"],
                conn_details["password"],
                conn_details["host"],
                conn_details["port"],
            )
        elif set(settings_local).issubset(conn_details):
            self.props = "dbname=%s user=%s" % (
                conn_details["db_name"],
                conn_details["user"],
            )
        try:
            self.conn = psycopg2.connect(self.props)
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

            self.__init__(conn_details, self.ttw * 2)

    def execute_cmd(self, cmd, values=None):
        try:
            if values is not None:
                self.cur.execute(cmd, values)
            else:
                self.cur.execute(cmd)
                self.conn.commit()

        except (psycopg2.InterfaceError, psycopg2.OperationalError) as exc:
            self.handle_interface_and_oper_error()

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

    def __del__(self):
        self.conn.close()
        self.cur.close()

    def handle_interface_and_oper_error(self):
        logger.error("[INIT_PG] Trying to reconnect to Postgres...")
        self.attempt_to_reconnect = True
        self.__init__(self.conn_details, self.ttw * 2)
