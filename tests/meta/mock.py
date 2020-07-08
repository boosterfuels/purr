import pymongo
from etl.load import init_pg as postgres
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from bson import Timestamp
import os


# ------ CONNECTION STRINGS ------
conn_str_pg = 'postgres://localhost:5432/'
conn_str_mongo = 'mongodb://localhost:27017'
NAME_DB = 'purr_test'
db_name_mongo = db_name_pg = NAME_DB
conn_str_pg = conn_str_pg + db_name_pg

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

schema = setup_pg["schema_name"]

# --- RELATION INFORMATION ---

coll_names = ['Company', 'Employee']
rel_names = ['company', 'employee']

rel_name_company = 'company'
rel_name_employee = 'employee'
collection_map = 'purr_collection_map'

attrs_company = ["id", "active", "domains", "signup_code"]
types_company = ["TEXT", "BOOLEAN", "JSONB", "TEXT"]

attrs_types_company = []
for i in range(len(attrs_company)):
    attrs_types_company.append("%s %s" % (attrs_company[i], types_company[i]))

attrs_employee = ["id", "first_name", "last_name", "hair"]
types_employee = ["TEXT", "TEXT", "TEXT", "TEXT"]

attrs_types_employee = []
for i in range(len(attrs_employee)):
    attrs_types_employee.append("%s %s" % (
        attrs_employee[i], types_employee[i]))


# --- COLLECTION INFORMATION ---
coll_name_company = 'Company'
fields_company = ["_id", "active", "domains", "signupCode"]

coll_name_employee = 'Employee'
fields_employee = ["firstName", "lastName", "hair"]


# --- QUERIES ---
query = {
    "db_exists": """select exists(SELECT datname
        FROM pg_catalog.pg_database
        WHERE lower(datname) = lower('%s'))""" % db_name_pg,
    "db_create": "CREATE DATABASE %s;" % db_name_pg,
    "db_drop": "DROP DATABASE IF EXISTS %s;" % db_name_pg,
    "table_drop_purr_cm": "DROP TABLE IF EXISTS %s;" % collection_map,
    "table_drop_company": "DROP TABLE IF EXISTS %s;" % rel_name_company,
    "table_drop_employee": "DROP TABLE IF EXISTS %s;" % rel_name_employee,
    "table_create_company": """CREATE TABLE %s(%s);""" % (
        rel_name_company, ', '.join(attrs_types_company)),
    "table_create_employee": "CREATE TABLE %s(%s);" % (
        rel_name_employee, ', '.join(attrs_types_employee)),
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


coll_config_db = [(0, 'Company', 'company', [
    {'id': None, ':type': 'TEXT', ':source': '_id'},
    {':type': 'BOOLEAN', 'active': None, ':source': 'active'},
    {':type': 'JSONB', ':source': 'domains', 'domains': None},
    {':type': 'TEXT', ':source': 'signupCode', 'signup_code': None}
])]

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

coll_config_db_new = [(0, 'Company', 'company', [
    {'id': None, ':type': 'TEXT', ':source': '_id'},
    {':type': 'BOOLEAN', 'active': None, ':source': 'active'},
    {':type': 'TEXT', ':source': 'domains', 'domains': None},
    {':type': 'TEXT', ':source': 'signupCode', 'signup_code': None}
])]


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
        "hair": "black"
    },
    {

        "firstName": "Arya",
        "lastName": "Start",
        "hair": "brown"
    },
    {

        "firstName": "Sansa",
        "lastName": "Stark",
        "hair": "ginger"
    },
    {

        "firstName": "Little",
        "lastName": "Finger",
        "hair": "dark brown"
    },
    {

        "firstName": "The",
        "lastName": "Hound",
        "hair": "brown"
    }
]


attr_details = {
    '_id': {'name_cm': 'id', 'type_cm': 'text', 'value': None},
    'active': {'name_cm': 'active', 'type_cm': 'boolean', 'value': None},
    'domains': {'name_cm': 'domains', 'type_cm': 'jsonb', 'value': None},
    'extraProps': {
        'name_cm': '_extra_props', 'type_cm': 'jsonb', 'value': None
    },
    'signupCode': {'name_cm': 'signup_code', 'type_cm': 'text', 'value': None}
}


attr_details_with_values = {
    '_id': {'name_cm': 'id',
            'type_cm': 'text',
            'value': '12345'},
    'active': {'name_cm': 'active',
               'type_cm': 'boolean',
               'value': True},
    'domains': {'name_cm': 'domains',
                'type_cm': 'jsonb',
                'value': {"one": "two"}},
    'extraProps': {'name_cm': '_extra_props',
                   'type_cm': 'jsonb',
                   'value': {"three": "four",
                             "five": "six"}
                   },
    'signupCode': {'name_cm': 'signup_code',
                   'type_cm': 'text',
                   'value': "I am a text"}
}


oplog_entries_update = [
    {
        'ts': Timestamp(1556029671, 1),
        't': 48,
        'h': -4473962510602026742,
        'v': 2,
        'op': 'u',
        'ns': 'test_purrito.Employee',
        'o2': {'_id': '1'},
        'o': {
            '$set': {
                'firstName': 'Janos',
                'lastName': None
            },
            '$unset': {
                'hair': True
            }
        },
    },
    {
        'ts': Timestamp(1556029671, 2),
        't': 48,
        'h': -6116078169406119246,
        'v': 2,
        'op': 'u',
        'ns': 'test_purrito.Company',
        'o2': {
            '_id': 2
        },
        'o': {
            '$set': {
                'domains': ['dragonglass.org']
            }
        }
    },
    {
        'ts': Timestamp(1556029671, 1),
        't': 48,
        'h': -4473962510602026742,
        'v': 2,
        'op': 'u',
        'ns': 'test_purrito.Employee',
        'o2': {'_id': '2'},
        'o': {
            '$set': {
                'firstName': 'Arja',
                "lastName": "Stark",
                'hair': 'blonde'
            }
        }
    },
]


def setup_pg_tables():
    cursor = pg.conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.purr_collection_map
        (
            id integer NOT NULL,
            collection_name text COLLATE pg_catalog."default",
            relation_name text COLLATE pg_catalog."default",
            types jsonb[],
            updated_at timestamp without time zone,
            query_update text COLLATE pg_catalog."default",
            CONSTRAINT purr_collection_map_pkey PRIMARY KEY (id)
        )
        WITH (
            OIDS = FALSE
        )
        TABLESPACE pg_default;

        DROP FUNCTION IF EXISTS public.notify_type();

        CREATE FUNCTION public.notify_type()
            RETURNS trigger
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE NOT LEAKPROOF
        AS $BODY$
            BEGIN
                PERFORM pg_notify('purr', 'type_change');
                RETURN NULL;
            END;
            $BODY$;

        CREATE TRIGGER notify
            AFTER INSERT OR DELETE OR UPDATE 
            ON public.purr_collection_map
            FOR EACH ROW
            EXECUTE PROCEDURE public.notify_type();

        CREATE SEQUENCE public.purr_error_id_seq
            INCREMENT 1
            START 74984
            MINVALUE 1
            MAXVALUE 2147483647
            CACHE 1;

        CREATE TABLE IF NOT EXISTS public.purr_error
        (
            id integer NOT NULL DEFAULT nextval('purr_error_id_seq'::regclass),
            location text COLLATE pg_catalog."default",
            message text COLLATE pg_catalog."default",
            ts integer
        )
        TABLESPACE pg_default;

        CREATE TABLE IF NOT EXISTS public.purr_info
        (
            latest_successful_ts text COLLATE pg_catalog."default"
        )
        TABLESPACE pg_default;

        CREATE SEQUENCE public.purr_oplog_id_seq
            INCREMENT 1
            START 1
            MINVALUE 1
            MAXVALUE 2147483647
            CACHE 1;

        CREATE TABLE IF NOT EXISTS public.purr_oplog
        (
            id integer NOT NULL DEFAULT nextval('purr_oplog_id_seq'::regclass),
            operation text COLLATE pg_catalog."default",
            relation text COLLATE pg_catalog."default",
            obj_id text COLLATE pg_catalog."default",
            ts integer NOT NULL,
            merged boolean,
            document text COLLATE pg_catalog."default",
            CONSTRAINT purr_oplog_pkey PRIMARY KEY (id, ts)
        )
        TABLESPACE pg_default;

        CREATE SEQUENCE public.purr_transfer_stats_id_seq
            INCREMENT 1
            START 438
            MINVALUE 1
            MAXVALUE 2147483647
            CACHE 1;
            
        CREATE TABLE IF NOT EXISTS public.purr_transfer_stats
        (
            id integer NOT NULL DEFAULT nextval('purr_transfer_stats_id_seq'::regclass),
            action text COLLATE pg_catalog."default",
            relation text COLLATE pg_catalog."default",
            number_of_rows integer,
            ts_start integer,
            ts_end integer
        )
        TABLESPACE pg_default;

        """)

setup_pg_tables()
