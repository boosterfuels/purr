import psycopg2
import pymongo
from etl import extract
from etl.extract import extractor
from etl.load import init_pg as postgres
from etl.transform import relation
import pytest
import unittest
from bson import ObjectId

# connect to databases
conn_str_mongo = 'mongodb://localhost:27017'
conn_str_pg = 'postgres://127.0.0.1:5432/postgres'

# create test database test_purr in PgSQL
# CREATE DATABASE test_purr;
db_name_mongo = db_name_pg = 'test_purr'

mongo_client = pymongo.MongoClient(conn_str_mongo)
mongo = mongo_client[db_name_mongo]

pg = postgres.PgConnection(conn_str_pg)

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

rel_name = 'company'
coll_name = 'Company'

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


def init():
    # create test databases in Mongo and Postgres
    create_dataset()


def create_dataset():
    cur = pg.conn.cursor()
    pg.conn.autocommit = True
    query = {
        "db_exists": "select exists(SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower(%s))" % db_name_pg,
        "db_create": "CREATE DATABASE %s;" % db_name_pg,
        "db_drop": "DROP DATABASE %s;" % db_name_pg
    }
    res = cur.execute(query["db_exists"])
    if res[0][0] is False:
        cur.execute(query["db_create"])
    # cur.execute(query["db_drop"])
    cur.close()


class TestExtractorInit(unittest.TestCase):

    # TODO: test init variables -> append default values
    def test_init(self):
        assert coll_config[coll_name][':meta'][':extra_props'] == 'JSONB'

    # def test_transfer_auto(self):
    #     assert (4+1) == 5

    def test_prepare_transfer_conf(self):
        # transfer collection
        cmd = "DROP TABLE IF EXISTS %s;" % rel_name
        pg.execute_cmd(cmd)

        ex = extractor.Extractor(pg, mongo, setup_pg, settings, coll_config)
        ex.prepare_transfer_conf([coll_name])
        cmd = "SELECT count(*) from %s;" % rel_name
        cnt_pg = pg.execute_cmd_with_fetch(cmd)
        cnt_mongo = mongo[coll_name].count()
        print(cnt_pg[0][0], cnt_mongo)
        assert cnt_pg[0][0] == cnt_mongo

    def test_transfer_coll(self):
        # transfer one collection
        cmd = "DROP TABLE IF EXISTS %s;" % rel_name
        pg.execute_cmd(cmd)

        ex = extractor.Extractor(pg, mongo, setup_pg, settings, coll_config)
        ex.transfer_coll(coll_name)

        cmd = "SELECT count(*) from %s;" % rel_name
        cnt_pg = pg.execute_cmd_with_fetch(cmd)
        cnt_mongo = mongo[coll_name].count()
        print(cnt_pg[0][0], cnt_mongo)
        assert cnt_pg[0][0] == cnt_mongo

    def test_transfer_doc(self):
        doc_orig = {
            "active": True,
            "domains": [],
            "signupCode": "12345",
        }
        doc = mongo[coll_name].insert_one(doc_orig)
        oid = doc.inserted_id
        doc = extract.collection.get_doc_by_id(mongo, coll_name, str(oid))
        cur = pg.conn.cursor()
        ex = extractor.Extractor(pg, mongo, setup_pg, settings, coll_config)
        r = relation.Relation(pg, setup_pg['schema_name'], coll_name)
        ex.transfer_doc(doc, r, coll_name)
        cmd = "SELECT id, active, domains, signup_code FROM %s WHERE id = '%s';" % (
            rel_name, oid)
        cur.execute(cmd)
        row = cur.fetchone()
        assert (str(doc["_id"]), doc["active"], doc["domains"],
                doc["signupCode"]) == row

    def test_prepare_attr_details_with_extra_props(self):
        # attributes from config file
        attrs_conf = ['id', 'active', 'domains', 'signup_code']
        # fields from mongodb
        attrs_mdb = ['_id', 'active', 'domains', 'signupCode']
        # types from config file
        types_conf = ['text', 'boolean', 'jsonb', 'text']
        type_extra_props_pg = 'jsonb'
        ex = extractor.Extractor(pg, mongo, setup_pg, settings, coll_config)
        ex.include_extra_props = True
        attribute_details = ex.prepare_attr_details(
            attrs_conf, attrs_mdb, types_conf, type_extra_props_pg)
        import pprint
        pprint.pprint(attribute_details)
        res = {
            '_id': {'name_conf': 'id', 'type_conf': 'text', 'value': None},
            'active': {'name_conf': 'active', 'type_conf': 'boolean', 'value': None},
            'domains': {'name_conf': 'domains', 'type_conf': 'jsonb', 'value': None},
            'extraProps': {'name_conf': '_extra_props', 'type_conf': 'jsonb', 'value': None},
            'signupCode': {'name_conf': 'signup_code', 'type_conf': 'text', 'value': None}
        }
        assert res == attribute_details

    def test_prepare_attr_details_without_extra_props(self):
        attrs_conf = ['id', 'active', 'domains', 'signup_code']
        attrs_mdb = ['_id', 'active', 'domains', 'signupCode']
        types_conf = ['text', 'boolean', 'jsonb', 'text']
        type_extra_props_pg = None
        ex = extractor.Extractor(pg, mongo, setup_pg, settings, coll_config)
        ex.include_extra_props = False
        attribute_details = ex.prepare_attr_details(
            attrs_conf, attrs_mdb, types_conf, type_extra_props_pg)
        import pprint
        pprint.pprint(attribute_details)
        res = {
            '_id': {'name_conf': 'id', 'type_conf': 'text', 'value': None},
            'active': {'name_conf': 'active', 'type_conf': 'boolean', 'value': None},
            'domains': {'name_conf': 'domains', 'type_conf': 'jsonb', 'value': None},
            'signupCode': {'name_conf': 'signup_code', 'type_conf': 'text', 'value': None}
        }
        assert res == attribute_details

    def test_adjust_columns(self):
        assert 1 == 1

# def f():
#     raise SystemExit(1)

# def test_mytest():
#     with pytest.raises(SystemExit):
#         f()

# class TestExtractor(unittest.TestCase):
#     def test_one(self):
#         x = "this"
#         assert 'h' in x
