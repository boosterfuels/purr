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

coll_names = ['Company', 'Employee']
rel_names = ['company', 'employee']

rel_name_company = 'company'
rel_name_employee = 'employee'
collection_map = 'purr_collection_map'

attrs_company = ["id", "active", "domains", "signup_code"]
types_company = ["TEXT", "BOOLEAN", "JSONB", "TEXT"]

attrs_types = []
for i in range(len(attrs_company)):
    attrs_types.append("%s %s" % (attrs_company[i], types_company[i]))

# --- COLLECTION INFORMATION ---
coll_name_company = 'Company'
fields_company = ["_id", "active", "domains", "signupCode"]

coll_name_employee = 'Employee'
fields_employee = ["firstName", "lastName", "hair"]
attrs_employee = ["first_name", "last_name", "hair"]


# --- QUERIES ---
query = {
    "db_exists": "select exists(SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('%s'))" % db_name_pg,
    "db_create": "CREATE DATABASE %s;" % db_name_pg,
    "db_drop": "DROP DATABASE IF EXISTS %s;" % db_name_pg,
    "table_drop_purr_cm": "DROP TABLE IF EXISTS %s;" % collection_map,
    "table_drop_company": "DROP TABLE IF EXISTS %s;" % rel_name_company,
    "table_drop_employee": "DROP TABLE IF EXISTS %s;" % rel_name_employee,
    "table_create_company": "CREATE TABLE %s(%s);" % (rel_name_company, ', '.join(attrs_types)),
    "table_check_company_columns": """
        SELECT DISTINCT column_name, data_type FROM information_schema.columns
        WHERE table_name = '%s'
    """ % rel_name_company
}

coll_config = {
    coll_name_company:
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
            ':table': rel_name_company
        }
    }
}


coll_config_db = [(0, 'Company', 'company', [{'id': None, ':type': 'TEXT', ':source': '_id'}, {':type': 'BOOLEAN', 'active': None, ':source': 'active'}, {
                   ':type': 'JSONB', ':source': 'domains', 'domains': None}, {':type': 'TEXT', ':source': 'signupCode', 'signup_code': None}])]

coll_config_new = {
    coll_name_company:
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
            ':table': rel_name_company
        }
    }
}

coll_config_db_new = [(0, 'Company', 'company', [{'id': None, ':type': 'TEXT', ':source': '_id'}, {':type': 'BOOLEAN', 'active': None, ':source': 'active'}, {
                       ':type': 'TEXT', ':source': 'domains', 'domains': None}, {':type': 'TEXT', ':source': 'signupCode', 'signup_code': None}])]


coll_config_company_employee = {
    coll_name_company:
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
            ':table': rel_name_company
        }
    },
    coll_name_employee:
    {
        ':columns': [
            {':source': '_id',
             ':type': 'TEXT',
             'id': None},
            {':source': 'firstName',
             ':type': 'TEXT',
             'first_name': None},
            {':source': 'lastName',
             ':type': 'TEXT',
             'last_name': None},
            {':source': 'hair',
             ':type': 'TEXT',
             'hair': None}],
        ':meta': {
            ':extra_props': 'JSONB',
            ':table': rel_name_employee
        }
    }
}

coll_config_db_company_employee = [
    (0, 'Company', 'company', [
        {'id': None, ':type': 'TEXT', ':source': '_id'},
        {':type': 'BOOLEAN', 'active': None, ':source': 'active'},
        {':type': 'JSONB', ':source': 'domains', 'domains': None},
        {':type': 'TEXT', ':source': 'signupCode', 'signup_code': None}
    ]),
    (1, 'Employee', 'employee', [
        {'id': None, ':type': 'TEXT', ':source': '_id'},
        {':type': 'TEXT', 'first_name': None, ':source': 'firstName'},
        {':type': 'TEXT', ':source': 'lastName', 'last_name': None},
        {':type': 'TEXT', ':source': 'hair', 'hair': None}
    ])
]


pg_coll_map_attrs = ["id", "collection_name", "relation_name",
                     "types", "updated_at", "query_update"]


data_mdb_company = [
    {
        "active": True,
        "signupCode": "uPsYdUpSy123",
        "domains": [
            "southpark.com"
        ]
    },
    {
        "active": True,
        "signupCode": "node",
        "domains": [
            "amazon.com"
        ]
    },
    {
        "active": True,
        "signupCode": "kInGsSpOrT32",
        "domains": [
            "stuff.com",
            "baddance.com",
            "chewbacca.com"
        ]
    },
    {
        "active": False,
        "signupCode": "BoOmClAp<3",
        "domains": [
            "festival.com"
        ]
    },
    {
        "active": False,
        "signupCode": "LiPGlOsS24",
        "domains": [
            "platform934.org",
            "hogwarts.com",
        ]
    }
]


# data should look like this after transfer
# if ordered by id
data_pg_company = [
    ('5ca7b1d3a54d75271eb97ab8', True, ['southpark.com'], 'uPsYdUpSy123'),
    ('5ca7b1d3a54d75271eb97ab9', True, ['amazon.com'], 'node'),
    ('5ca7b1d3a54d75271eb97aba', True, [
     'stuff.com', 'baddance.com', 'chewbacca.com'], 'kInGsSpOrT32'),
    ('5ca7b1d3a54d75271eb97abb', False, ['festival.com'], 'BoOmClAp<3'),
    ('5ca7b1d3a54d75271eb97abc', False, [
     'platform934.org', 'hogwarts.com'], 'LiPGlOsS24')
]
data_pg_company_no_id = [
    (True, ['southpark.com'], 'uPsYdUpSy123'),
    (True, ['amazon.com'], 'node'),
    (True, ['stuff.com', 'baddance.com', 'chewbacca.com'], 'kInGsSpOrT32'),
    (False, ['festival.com'], 'BoOmClAp<3'),
    (False, ['platform934.org', 'hogwarts.com'], 'LiPGlOsS24')
]


data_mdb_company_updated = {
    "active": True,
    "signupCode": "uPsYdUpSy123",
    "domains": [
        "southpark.com"
    ]
}


data_pg_company_updated_no_id = [
    (True, ['southpark.com'], 'uPsYdUpSy123'),
    (True, ['southpark.com'], 'uPsYdUpSy123'),
    (True, ['southpark.com'], 'uPsYdUpSy123'),
    (True, ['southpark.com'], 'uPsYdUpSy123'),
    (True, ['southpark.com'], 'uPsYdUpSy123'),
]


data_mdb_employee = [
    {
        "firstName": "John",
        "lastName": "Snow",
    },
    {

        "firstName": "Arya",
        "lastName": "Start",
    },
    {

        "firstName": "Sansa",
        "lastName": "Stark",
    },
    {

        "firstName": "Little",
        "lastName": "Finger",
    },
    {

        "firstName": "The",
        "lastName": "Hound",
    }
]


attr_details = {
    '_id': {'name_conf': 'id', 'type_conf': 'text', 'value': None},
    'active': {'name_conf': 'active', 'type_conf': 'boolean', 'value': None},
    'domains': {'name_conf': 'domains', 'type_conf': 'jsonb', 'value': None},
    'extraProps': {'name_conf': '_extra_props', 'type_conf': 'jsonb', 'value': None},
    'signupCode': {'name_conf': 'signup_code', 'type_conf': 'text', 'value': None}
}
