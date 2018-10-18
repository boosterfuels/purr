import psycopg2
import pymongo
from etl import extract
from etl.load import init_pg as postgres
from etl.extract import extractor
import pytest
import unittest

# connect to databases
conn_str_mongo = 'mongodb://localhost:27017'
conn_str_pg = 'postgres://127.0.0.1:5432/test_purr'

# create test database test_purr in PgSQL
# CREATE DATABASE test_purr;
db_name_mongo = db_name_pg = 'test_purr'

mongo_client = pymongo.MongoClient(conn_str_mongo) 
mongo = mongo_client[db_name_mongo]

pg = postgres.PgConnection(conn_str_pg)


# create test databases in Mongo and Postgres
# setup_pg = 
# settings = 
# coll_config = 
setup_pg = {
    'connection': '', 
    'schema_name': '', 
    'schema_reset': None, 
    'table_truncate': None, 
    'table_drop': None
}

settings  = {
    'tailing': None,
    'tailing_from': None,
    'tailing_from_db': None,
    'typecheck_auto': None,
    'include_extra_props': None
}

coll_config = {
    'Company': 
        {
            ':columns': [
                {':source': '_id', ':type': 'TEXT', 'id': None},
                {':source': 'active',
                ':type': 'BOOLEAN',
                'active': None},
                {':source': 'domains',
                ':type': 'JSONB',
                'domains': None},
                {':source': 'signupCode',
                ':type': 'JSONB',
                'signup_code': None}],
            ':meta': {
                ':extra_props': 'JSONB', 
                ':table': 'company'
            }
        }
}

rel_name = 'company'

class TestExtractorInit(unittest.TestCase):

    # TODO: test init variables -> append default values
    def test_init(self):
        assert coll_config['Company'][':meta'][':extra_props'] == 'JSONB' 

    # def test_transfer_auto(self):
    #     coll_names = ['Company']
    #     ex.transfer_auto(coll_names)
    #     assert (4+1) == 5

    def test_transfer_conf(self):
        coll_names = ['Company']
        cmd = "DROP TABLE IF EXISTS %s;" % (rel_name)
        ex = extractor.Extractor(pg, mongo, setup_pg, settings, coll_config)
        pg.execute_cmd(cmd)

        ex.transfer_conf(coll_names)
        cmd = "SELECT count(*) from %s;" % rel_name
        cnt_pg = pg.execute_cmd_with_fetch(cmd)
        cnt_mongo = mongo['Company'].count()
        assert cnt_pg[0][0] == cnt_mongo

    def test_transfer_coll(self):
        coll_name = 'Company'
        cmd = "DROP TABLE IF EXISTS %s;" % rel_name
        pg.execute_cmd(cmd)

        ex = extractor.Extractor(pg, mongo, setup_pg, settings, coll_config)
        ex.transfer_coll(coll_name)

        cmd = "SELECT count(*) from %s;" % rel_name
        cnt_pg = pg.execute_cmd_with_fetch(cmd)
        cnt_mongo = mongo['Company'].count()
        assert cnt_pg[0][0] == cnt_mongo

    def test_transfer_doc(self):
        # doc =  
        # r = 
        # coll = 
        assert 1 == 1

    def test_prepare_attr_details(self):
        assert 1 == 1

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
