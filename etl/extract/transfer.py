from threading import Thread
from etl.extract import tailer, transfer_info
from etl.transform import config_parser as cp
from etl.load import schema
from datetime import datetime
from etl.monitor import logger


def start(extractor, coll_config):
    collections = cp.config_collection_names(coll_config)
    if collections is None:
        logger.error(
            """
                [TRANSFER] No collections found.
                Check your collection names in the setup file.
                """)
        return
    extractor.transfer(collections)


class TransferThread(Thread):
    new = False

    def __init__(self, settings, coll_config, pg, mongo, ex, tail, dt=None):
        Thread.__init__(self)
        self.settings = settings
        self.coll_config = coll_config
        self.pg = pg
        self.mongo = mongo
        self.t = tailer.Tailer(self.pg, self.mongo,
                               self.settings["postgres"],
                               self.settings,
                               self.coll_config)
        # initialize extractor
        self.extractor = ex
        self.terminate = False
        self.dt = dt
        self.start_tailing = tail

    def run(self):
        # collections which will be transferred
        if self.start_tailing is False:
            setup_pg = self.settings["postgres"]

            tailing_cmd = self.settings["tailing_from"]
            tailing_db = self.settings["tailing_from_db"]
            if tailing_db is False and tailing_cmd is None:

                schema.create(self.pg, setup_pg["schema_name"])

                # after extracting all collections tailing
                # will start from this timestamp
                if setup_pg["schema_reset"] is True:
                    schema.reset(self.pg, setup_pg["schema_name"])

                transfer_info.save_logs_to_db(self.pg, setup_pg["schema_name"])

                start(self.extractor, self.coll_config)
        else:
            self.tail(self.dt)

    def stop(self):
        self.terminate = True
        self.t.stop_tailing = True

    def tail(self, start_date_time=None):
        self.t.stop_tailing = False

        tailing_cmd = self.settings["tailing_from"]
        tailing_db = self.settings["tailing_from_db"]

        if self.settings["tailing"] is True:
            logger.info("[TRANSFER] Starting standard tailing.")
            self.t.start(start_date_time)

        elif tailing_cmd is not None:
            logger.info("[TRANSFER] Starting tailing from provided timestamp.")
            self.t.start(tailing_cmd)

        elif tailing_db is True or start_date_time is None:
            logger.info(
                """[TRANSFER] Starting tailing from
                timestamp found in purr_info.""")
            ts = transfer_info.get_latest_successful_ts(self.pg, 'public')
            latest_ts = int((list(ts)[0])[0])
            self.t.start(latest_ts)
        else:
            self.pg.__del__()

    def tailing_stop(self):
        self.t.stop_tailing = True

    def schema_update(self):
        self.extractor.update_coll_map()
