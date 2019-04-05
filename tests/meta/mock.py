import pymongo
from etl.load import init_pg as postgres
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from etl.extract import extractor, extractor


# ------ CONNECTION STRINGS ------
conn_str_mongo = 'mongodb://localhost:27017'
conn_str_pg = 'postgres://127.0.0.1:5432'


NAME_DB = 'test_purrito'
db_name_mongo = db_name_pg = NAME_DB

# ------ CONNECTION ------

mongo_client = pymongo.MongoClient(conn_str_mongo)
mongo = mongo_client[db_name_mongo]

pg = postgres.PgConnection(conn_str_pg)
pg.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

# ----- PURR -----
# --- SETTINGS ---
setup_pg = {
    'connection': '',
    'schema_name': 'public',
    'schema_reset': None,
    'table_truncate': None,
    'table_drop': None
}

settings = {
    'tailing': None,
    'tailing_from': None,
    'tailing_from_db': None,
    'typecheck_auto': None,
    'include_extra_props': None
}

# --- RELATION INFORMATION ---

rel_name = 'company'
rel_name_coll_map = 'purr_collection_map'

company_attrs = ["id", "active", "domains", "signup_code"]
company_types = ["TEXT", "BOOLEAN", "JSONB", "TEXT"]

attrs_types = []
for i in range(len(company_attrs)):
    attrs_types.append("%s %s" % (company_attrs[i], company_types[i]))

# --- COLLECTION INFORMATION ---
coll_name = 'Company'
company_fields = ["active", "domains", "signupCode"]

# --- QUERIES ---
query = {
    "db_exists": "select exists(SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('%s'))" % db_name_pg,
    "db_create": "CREATE DATABASE %s;" % db_name_pg,
    "db_drop": "DROP DATABASE IF EXISTS %s;" % db_name_pg,
    "table_drop_purr_cm": "DROP TABLE IF EXISTS %s;" % rel_name_coll_map,
    "table_drop_company": "DROP TABLE IF EXISTS %s;" % rel_name,
    "table_create_company": "CREATE TABLE %s(%s);" % (rel_name, ', '.join(attrs_types)),
    "table_check_company_columns": """
        SELECT DISTINCT column_name, data_type FROM information_schema.columns 
        WHERE table_name = '%s'
    """ % rel_name
}

coll_config = {
    coll_name:
    {
        ':columns': [
              {':source': '_id',
               ':type': 'TEXT',
               'id': None},
              {':source': 'active',
               ':type': 'BOOLEAN',
               'active': None},
              {':source': 'domains',
               ':type': 'JSONB',
               'domains': None},
              {':source': 'signupCode',
               ':type': 'TEXT',
               'signup_code': None}],
        ':meta': {
            ':extra_props': 'JSONB',
            ':table': rel_name
        }
    }
}


coll_config_db = [(0, 'Company', 'company', [{'id': None, ':type': 'TEXT', ':source': '_id'}, {':type': 'BOOLEAN', 'active': None, ':source': 'active'}, {
                   ':type': 'JSONB', ':source': 'domains', 'domains': None}, {':type': 'TEXT', ':source': 'signupCode', 'signup_code': None}])]

coll_config_new = {
    coll_name:
    {
        ':columns': [
            {':source': '_id',
             ':type': 'TEXT',
             'id': None},
            {':source': 'active',
             ':type': 'BOOLEAN',
             'active': None},
            {':source': 'domains',
             ':type': 'TEXT',
             'domains': None},
            {':source': 'signupCode',
             ':type': 'TEXT',
             'signup_code': None}],
        ':meta': {
            ':extra_props': 'JSONB',
            ':table': rel_name
        }
    }
}

coll_config_db_new = [(0, 'Company', 'company', [{'id': None, ':type': 'TEXT', ':source': '_id'}, {':type': 'BOOLEAN', 'active': None, ':source': 'active'}, {
                       ':type': 'TEXT', ':source': 'domains', 'domains': None}, {':type': 'TEXT', ':source': 'signupCode', 'signup_code': None}])]

ex = extractor.Extractor(pg, mongo, setup_pg, settings, coll_config)

pg_coll_map_attrs = ["id", "collection_name", "relation_name",
                     "types", "updated_at", "query_update"]