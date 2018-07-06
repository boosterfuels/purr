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
        description = '''An ETL tool to transfer data from MongoDB to Postgres.'''
    )
    
    parser.add_argument('-sf', '--setup-file', type=str,  dest='path_setup_file', help='Path to YAML file bith basic setup: \nconnection string for MongoDB and PG, schema name, schema reset, collections to transfer, etc.')
    parser.add_argument('-cf', '--collection-file', type=str, dest='path_collection_file', help='Path to YAML file that contains information about collections: collection names, field names, types, PG relation and attribute names, etc.')
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
            etl.core.start(setup_file, coll_file)
