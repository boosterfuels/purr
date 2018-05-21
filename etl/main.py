import argparse
import sys
import core
from datetime import datetime
import monitor
from transform import config_parser

setup = {}

def valid_date(s):
    try:
        return datetime.strptime(s, "%d-%m-%yT%H:%M:%S")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

parser = argparse.ArgumentParser(
  description = '''An ETL tool for your MongoDB and Postgres.
  '''
  )

parser.add_argument('-lc', '--list-colls', action='store_true', dest='list_colls', help='List all collections in MongoDB.')
parser.add_argument('-c', '--colls', type=str,  dest='mdb_colls', help='MongoDB collections which will be transfered. Case sensitive!', nargs='+')
parser.add_argument('-cf', '--config-file', type=str,  dest='path_conf_file', help='Basic setup like schema name, collections to transfer.')
parser.add_argument('-a', '--all', action='store_true', dest='transfer_all', help='Transfer all collections from MongoDB to PG')
parser.add_argument('-t', '--tail', action='store_true', dest='tail', help='Start tailing the oplog')
parser.add_argument('-rs', '--reset-schema', action='store_true', dest='reset_schema', help='Reset schema in PG')
parser.add_argument('-tr', '--truncate', action='store_true', dest='truncate', help='Clear existing tables')
parser.add_argument('-dr', '--drop', action='store_true', dest='drop', help='Drop existing tables')
parser.add_argument("-s", 
                    "--start",
                    dest="start_date_time",
                    help="Start date and time; \nuse in combination with --tail in order to tail the oplog from a specific date and time - format YY-MM-DDTHH:MM:SS. Example 18-04-18T16:30:00", 
                    type=valid_date)

try:
    args = parser.parse_args()

    if len(sys.argv) <= 1:
        sys.argv.append('--help')

    # print(args)

    if args.reset_schema:
        core.reset_schema()

    elif args.mdb_colls:
        res = []
        mdb_colls = args.mdb_colls[0].split(",")
        for c in mdb_colls:
            res.append(c)
        core.transfer_collections(res, args.truncate, args.drop)

    elif args.transfer_all:
        colls = core.get_collection_names()
        core.transfer_collections(colls, args.truncate, args.drop)
    
    elif args.list_colls:
        print('List of collections in MongoDB:')
        for c in core.get_collection_names():
            print(c)

    if args.tail:
        core.start_tailing(args.start_date_time)

    elif args.path_conf_file:
        setup = config_parser.file_to_dict(args.path_conf_file)

        if setup:
            settings_postgres = setup["postgres"]
            colls = setup["collections"]
            if len(colls) == 0:
                colls = core.get_collection_names()
            core.transfer_collections(colls, args.truncate, args.drop, setup)

except KeyboardInterrupt:
    print("\nInterrupted by user.")
