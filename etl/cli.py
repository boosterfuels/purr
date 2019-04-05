import argparse
import sys
import time
from datetime import datetime

import etl.core
from etl.transform import config_parser

setup = {}


def valid_date(s):
    try:
        t = datetime.strptime(s, "%d-%m-%yT%H:%M:%S")
        epoch_time = int(time.mktime(t.timetuple()))
        return epoch_time
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def main():
    parser = argparse.ArgumentParser(
        description="""
        An ETL tool to transfer data from MongoDB to Postgres.
        """
    )

    parser.add_argument('-sf', '--setup-file', type=str,
                        dest="path_setup_file",
                        help="""
                        Path to YAML file bith basic setup: \n
                        connection string for MongoDB and PG,
                        schema name, schema reset, collections to
                        transfer, etc.""")
    parser.add_argument('-cf', '--collection-file', type=str,
                        dest='path_collection_file',
                        help="""
                        Path to YAML file that contains information about
                        collections: collection names, field names, types,
                        PG relation and attribute names, etc.
                        """)
    parser.add_argument('-td', '--table-drop', action='store_true',
                        dest='table_drop', default=False, help='')
    parser.add_argument('-tt', '--table-truncate', action='store_true',
                        dest='table_truncate', default=False, help='')
    parser.add_argument('-sr', '--schema-reset', action='store_true',
                        dest='schema_reset', default=False, help='')
    parser.add_argument('-sn', '--schema-name', type=str,
                        dest='schema_name', default='public', help='')
    parser.add_argument('-pg', '--pg-connection', type=str,
                        dest='pg_connection', help='')
    parser.add_argument('-mdb', '--mongo-connection', type=str,
                        dest='mongo_connection', default='', help='')
    parser.add_argument('-n', '--mongo-db-name', type=str,
                        dest='mongo_db_name', default='', help='')
    parser.add_argument('-t', '--tail', action='store_true',
                        dest='tail', default=False, help='')
    parser.add_argument('-m', '--map', action='store_true',
                        dest='map', help='Generate a collection map')
    parser.add_argument("-s",
                        "--start",
                        dest="timestamp",
                        nargs='?',
                        help="""Start tailing from specific timestamp; \n
                            use in combination with --tail in order to
                            tail the oplog from a specific date and time.
                            Format: DD-MM-YYThh:mm:ss.
                            Example: 25-07-18T16:30:00""",
                        type=valid_date)
    parser.add_argument('-tsdb', '--start-from-tsdb', action='store_true',
                        dest='timestamp_db', default=False,
                        help="""
                        Start tailing from latest saved timestamp.
                        Looks for the value saved in table purr_info.
                        """)

    parser.add_argument('-ta', '--typecheck-auto', action='store_true',
                        dest='typecheck_auto', default=False, help='')
    parser.add_argument('-ex', '--include-extra-properties',
                        action='store_true',
                        dest='include_extra_props',
                        default=False, help='')

    args = parser.parse_args()

    if len(sys.argv) <= 1:
        sys.argv.append('--help')

    setup_file = {}
    coll_file = {}
    if args.map is True and args.mongo_connection and args.mongo_db_name:
        etl.core.generate_collection_map({
            'connection': args.mongo_connection,
            'db_name': args.mongo_db_name
        })
    else:
        if args.path_collection_file:
            coll_file = config_parser.config_collections(
                args.path_collection_file)
        if args.pg_connection and args.mongo_connection and args.mongo_db_name:
            setup = {
                'postgres':
                {
                    'connection': args.pg_connection,
                    'schema_name': args.schema_name,
                    'schema_reset': args.schema_reset,
                    'table_truncate': args.table_truncate,
                    'table_drop': args.table_drop
                },
                'mongo':
                {
                    'connection': args.mongo_connection,
                    'db_name': args.mongo_db_name
                },
                'tailing': args.tail,
                'tailing_from': args.timestamp,
                'tailing_from_db': args.timestamp_db,
                'typecheck_auto': args.typecheck_auto,
                'include_extra_props': args.include_extra_props
            }

            # TODO this is bad ↓ merge tailing
            tailing_from_ts = setup["tailing_from"] or setup["tailing_from_db"]
            if setup["tailing"] and tailing_from_ts:
                print("\nOnly one tailing option is allowed.")
                raise SystemExit()
            etl.core.start(setup, coll_file)

        elif args.path_setup_file:
            setup_file = config_parser.config_basic(args.path_setup_file)
            if setup_file and coll_file:
                etl.core.start(setup_file, coll_file)
        else:
            print("Check your input...")
