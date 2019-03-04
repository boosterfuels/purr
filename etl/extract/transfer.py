from threading import Thread
from etl.extract import collection, extractor, tailer, init_mongo as mongodb, transfer_info, transfer, notification
from etl.transform import config_parser as cp
from etl.load import schema, init_pg as postgres
from datetime import datetime
from etl.monitor import logger

class TransferThread(Thread):
    new = False

    def __init__(self, settings, coll_config, pg, mongo, ex):
        Thread.__init__(self)
        self.settings = settings
        self.coll_config = coll_config
        self.pg = pg
        self.mongo = mongo
        self.t = tailer.Tailer(self.pg, self.mongo,
                               self.settings["postgres"], self.settings, self.coll_config)
        # initialize extractor
        self.extractor = ex
        
    def run(self):
        # collections which will be transferred
        setup_pg = self.settings["postgres"]
        setup_mdb = self.settings["mongo"]

        collections = cp.config_collection_names(self.coll_config)

        if collections is None:
            logger.error(
                "[CORE] No collections found. Check your collection names in the setup file.")
            return

        start_date_time = datetime.utcnow()

        if self.settings["tailing_from_db"] is False and self.settings["tailing_from"] is None:

            schema.create(self.pg, setup_pg["schema_name"])

            # after extracting all collections tailing will start from this timestamp

            if setup_pg["schema_reset"] is True:
                schema.reset(self.pg, setup_pg["schema_name"])

            transfer_info.create_stat_table(self.pg, setup_pg["schema_name"])
            transfer_info.create_coll_map_table(
                self.pg, setup_pg["schema_name"], self.coll_config)

        # Skip collection transfer if started in tailing mode.
            if self.extractor.typecheck_auto is True:
                self.extractor.transfer_auto(collections)
            else:
                self.extractor.transfer_conf(collections)
        self.tail(start_date_time)

    def tail(self, start_date_time=None):
        self.t.stop_tailing = False

        if self.settings["tailing"] is True:
            logger.info("Starting standard tailing.")
            self.t.start(start_date_time)

        elif self.settings["tailing_from"] is not None:
            logger.info("Starting tailing from provided timestamp.")
            self.t.start(settings["tailing_from"])

        elif self.settings["tailing_from_db"] is True or start_date_time is None:
            logger.info("Starting tailing from timestamp found in purr_info.")
            ts = transfer_info.get_latest_successful_ts(self.pg, 'public')
            latest_ts = int((list(ts)[0])[0])
            self.t.start(latest_ts)
        else:
            self.pg.__del__()

    def tailing_stop(self):
        self.t.stop_tailing = True

    def schema_update(self):
        self.extractor.update_coll_settings()
