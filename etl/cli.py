import argparse
import sys
from datetime import datetime

import etl.core
from etl.transform import config_parser

setup = {}

def valid_date(s):
    try:
        return datetime.strptime(s, "%d-%m-%yT%H:%M:%S")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def main():
    parser = argparse.ArgumentParser(
        description = '''An ETL tool for your MongoDB and Postgres.'''
    )
    
    parser.add_argument('-lc', '--list-colls', action='store_true', dest='list_colls', help='List all collections in MongoDB.')
    parser.add_argument('-c', '--colls', type=str,  dest='mdb_colls', help='MongoDB collections which will be transfered. Case sensitive!', nargs='+')
    parser.add_argument('-sf', '--setup-file', type=str,  dest='path_setup_file', help='Basic setup like schema name, collections to transfer.')
    parser.add_argument('-cf', '--collection-file', type=str, dest='path_collection_file', help='Path to the file that should contain collection names with corresponding field names MongoDB and PG.')
    args = parser.parse_args()

    if len(sys.argv) <= 1:
        sys.argv.append('--help')

    setup_file = {}
    coll_file = {}
    if args.path_setup_file:
        setup_file = config_parser.config_basic(args.path_setup_file)
    if args.path_collection_file:
        coll_file = config_parser.config_collections(args.path_collection_file)

        if setup_file and coll_file:
            settings_postgres = setup_file["postgres"]
            colls = setup_file["collections"]
            if len(colls) == 0:
                colls = core.get_collection_names()
            etl.core.transfer_collections(colls, setup_file, coll_file)
