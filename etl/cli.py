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
    parser.add_argument('-td', '--table-drop', action='store_true', dest='table_drop', default=False, help='')
    parser.add_argument('-tt', '--table-truncate', action='store_true', dest='table_truncate', default=False, help='')
    parser.add_argument('-sr', '--schema-reset', action='store_true', dest='schema_reset', default=False, help='')
    parser.add_argument('-sn', '--schema-name', type=str, dest='schema_name', default='public', help='')
    parser.add_argument('-pg', '--pg-connection', type=str, dest='pg_connection', help='')
    parser.add_argument('-mdb', '--mongo-connection', type=str, dest='mongo_connection', default='', help='')
    parser.add_argument('-n', '--mongo-db-name', type=str, dest='mongo_db_name', default='', help='')
    parser.add_argument('-t', '--tail', action='store_true', dest='tail', default=False, help='')
    parser.add_argument('-ta', '--typecheck-auto', action='store_true', dest='typecheck_auto', default=False, help='')
    args = parser.parse_args()

    if len(sys.argv) <= 1:
        sys.argv.append('--help')

    setup_file = {}
    coll_file = {}
    if args.path_collection_file:
        coll_file = config_parser.config_collections(args.path_collection_file)
        
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
            'typecheck_auto': args.typecheck_auto
        }
        etl.core.start(setup, coll_file)


    elif args.path_setup_file:
            setup_file = config_parser.config_basic(args.path_setup_file)
            if setup_file and coll_file:
                etl.core.start(setup_file, coll_file)
    else:
        print("Check your input...")
