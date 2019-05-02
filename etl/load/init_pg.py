import psycopg2
import psycopg2.extras
import time
from etl.monitor import logger
from etl.extract import transfer_info
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from enum import Enum

CURR_FILE = "[INIT_PG]"


class FunctionLatest(Enum):
    EXECUTE = 1,
    EXECUTE_WITH_FETCH = 2,
    EXECUTE_MANY = 3


class PgConnection:

    def __init__(self, conn_details, ttw=1):
        logger.info("%s Connecting to %s" % (CURR_FILE, conn_details))

        # time to wait before attempt to reconnect
        self.ttw = ttw
        self.conn_details = conn_details
        self.cmd_latest = None
        self.values_latest = None
        self.function_latest = None
        self.query_failed = False

        if ttw == 1:
            self.attempt_to_reconnect = False
        try:
            self.conn = psycopg2.connect(self.conn_details)
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            self.cur = self.conn.cursor()
            logger.info("%s Connected to Postgres." % (CURR_FILE))
            self.ttw = 1
            if self.query_failed is True:
                """
                the latest command should be repeated because Postgres
                was disconnected and it is likely that it was during a
                command
                """
                self.query_failed = False
                if self.function_latest is FunctionLatest.EXECUTE:
                    self.execute_cmd(self.cmd_latest, self.values_latest)
                elif self.function_latest is FunctionLatest.EXECUTE_WITH_FETCH:
                    self.execute_cmd_with_fetch(
                        self.cmd_latest, self.values_latest)
                elif self.function_latest is FunctionLatest.EXECUTE_MANY:
                    self.execute_many_cmd(self.cmd_latest, self.values_latest)

        except Exception as ex:
            self.attempt_to_reconnect = True
            msg = """
                %s Could not connect to Postgres.
                Reconnecting in %s seconds...
                Details: %s
                """ % (CURR_FILE, self.ttw, ex)
            self.log_error_in_pg(msg)
            time.sleep(self.ttw)

            self.__init__(self.conn_details, self.ttw * 2)

    def execute_cmd(self, cmd, values=None):
        self.cmd_latest = cmd
        self.values_latest = values
        self.function_latest = FunctionLatest.EXECUTE

        cur = self.conn.cursor()
        try:
            if values is not None:
                cur.execute(cmd, values)
            else:
                cur.execute(cmd)

        except (psycopg2.InterfaceError, psycopg2.OperationalError) as ex:
            self.handle_interface_and_oper_error()
            self.log_error_in_pg(ex)
            self.query_failed = True
        except Exception as ex:
            self.query_failed = True
            msg = """
                %s Executing query without fetch failed.
                Details: %s
                """ % (CURR_FILE, ex)
            self.log_error_in_pg(msg)
        cur.close()

    def execute_cmd_with_fetch(self, cmd, values=None):
        self.cmd_latest = cmd
        self.values_latest = values
        self.function_latest = FunctionLatest.EXECUTE_WITH_FETCH

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
            self.query_failed = True
            msg = """
                %s Executing query with fetch failed.
                Details: %s
                """ % (CURR_FILE, ex)
            self.log_error_in_pg(msg)

        cur.close()

    def execute_many_cmd(self, cmd, values):
        self.cmd_latest = cmd
        self.values_latest = values
        self.function_latest = FunctionLatest.EXECUTE_MANY

        cur = self.conn.cursor()
        try:
            cur.executemany(cmd, values)
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            self.handle_interface_and_oper_error()
        except Exception as ex:
            self.query_failed = True
            msg = """
                %s Executing many failed.
                Details: %s
                """ % (CURR_FILE, ex)
            self.log_error_in_pg(msg)
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
        msg = "%s Trying to reconnect to Postgres..." % (CURR_FILE)
        self.log_error_in_pg(msg)
        self.attempt_to_reconnect = True
        self.__init__(self.conn_details, self.ttw * 2)

    def log_error_in_pg(self, msg):
        logger.error(msg)
        msg_full = """Message: %s\nCommand: %s\nValues: %s""" % (
            msg, self.cmd_latest, self.values_latest)
        row = tuple(['PG', msg_full, time.time()])
        transfer_info.log_error(self, row)
