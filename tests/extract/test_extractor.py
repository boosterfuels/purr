import psycopg2
import pymongo
# from etl import extract
from etl.extract import extractor, extractor
from etl.transform import relation
import pytest
import unittest
from bson import ObjectId
from tests.meta import mock
from etl.extract import collection_map as cm
import copy

pg = mock.pg
mongo = mock.mongo
query = mock.query
rel_name_company = mock.rel_name_company
rel_name_company_cm = mock.rel_name_company_coll_map
coll_name_company = mock.coll_name_company
coll_name_employee = mock.coll_name_employee
coll_conf = mock.coll_config
coll_conf_new = mock.coll_config_new
ex = extractor.Extractor(pg, mongo, mock.setup_pg, mock.settings, coll_conf)
pg_cm_attrs = mock.pg_coll_map_attrs


def create_and_populate_company_pg():
    cursor = pg.conn.cursor()
    cursor.execute(query["table_drop_purr_cm"])
    # create table for CM in the database
    cm.create_table(pg, coll_conf)

    cursor.execute(query["table_drop_company"])
    cursor.execute(query["table_create_company"])

    cursor.execute(
        """insert into company(id, active, domains, signup_code) values('12345', 'true', '{"domain": ["pelotonland.com"]}', 'xfLfdsFD3S')""")

    cursor.close()


def create_and_populate_company_mdb():
    # TODO
    mongo.drop_collection(coll_name_company)
    print("creates collection company")
    docs = [
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
    mongo[coll_name_company].insert_many(docs)


def create_and_populate_employee_mdb():
    mongo.drop_collection(coll_name_employee)
    print("creates collection employee")
    docs = [
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
    mongo[coll_name_employee].insert_many(docs)


class TestExtractor(unittest.TestCase):

    def test_init_db(self):
        # create test databases in Mongo and Postgres
        # Postgres
        cursor = pg.conn.cursor()
        # cursor.execute(query["db_drop"])
        # cursor.execute(query["db_create"])
        cursor.execute(query["db_exists"])
        db_exists = cursor.fetchone()
        assert db_exists[0] == True
        cursor.close()

    def test_update_table_def_change_type(self):
        # Check if column was changed
        # update domain type: jsonb to text
        res_expected = [('active', 'boolean'), ('domains', 'text'),
                        ('id', 'text'), ('signup_code', 'text')]

        create_and_populate_company_pg()
        cursor = pg.conn.cursor()

        # update type
        ex.update_table_def(mock.coll_config_db, mock.coll_config_db_new)

        # get columns and types from information schema
        cursor.execute(query["table_check_company_columns"])
        res_db = cursor.fetchall()

        print(res_db)
        print(res_expected)
        cursor.close()
        assert res_db == res_expected

    def test_update_table_def_add_column(self):
        res_expected = [('active', 'boolean'), ('domains', 'jsonb'),
                        ('id', 'text'), ('signup_code', 'text'), ('updated_at', 'timestamp without time zone')]

        create_and_populate_company_pg()
        cursor = pg.conn.cursor()

        # add type
        conf = mock.coll_config_db
        conf_new = copy.deepcopy(conf)

        conf_new[0][3].append({
            ':source': 'updatedAt',
            ':type': 'TIMESTAMP',
            'updated_at': None
        })

        ex.update_table_def(conf, conf_new)

        # get columns and types from information schema
        cursor.execute(query["table_check_company_columns"])
        res_db = cursor.fetchall()

        print(res_db)
        print(res_expected)

        cursor.close()
        assert res_db == res_expected

    def test_update_table_def_remove_column(self):
        # Removing last column (signup code)
        res_expected = [('active', 'boolean'), ('domains', 'jsonb'),
                        ('id', 'text')]

        create_and_populate_company_pg()
        cursor = pg.conn.cursor()

        # add type
        conf = mock.coll_config_db
        conf_new = copy.deepcopy(conf)

        conf_new[0][3].pop()

        ex.update_table_def(conf, conf_new)

        # get columns and types from information schema
        cursor.execute(query["table_check_company_columns"])
        res_db = cursor.fetchall()

        print(res_db)
        print(res_expected)

        cursor.close()
        assert res_db == res_expected

    def test_table_track(self):
        # TODO: see if the table is transfered to the PG database
        create_and_populate_company_mdb()
        create_and_populate_employee_mdb()

        cursor = pg.conn.cursor()
        cursor.execute(query["table_drop_company"])
        cursor.execute(query["table_drop_employee"])
        cursor.execute(query["table_drop_purr_cm"])

        # create table for CM in the database
        cm.create_table(pg, coll_conf)
        ex = extractor.Extractor(
            pg, mongo, mock.setup_pg, mock.settings, coll_conf)

        # # changes extractor's collection definition for every collection
        # # and transfers

        coll_map_old = mock.coll_config_db
        coll_map_new = mock.coll_config_db_company_employee
        ex.table_track(coll_map_old, coll_map_new)

        mocked = mock.coll_config_company_employee
        if (len(ex.coll_def) != len(mocked)):
            assert False

        for k, v in ex.coll_def.items():
            if v != mocked[k]:
                print(k)
                print(v[":columns"])
                print(mocked[k][":columns"])
                if v[":columns"] != mocked[k][":columns"]:
                    assert False

        cursor.close()
        del ex

        assert True

    def test_table_untrack(self):
        # TODO:
        # - check if the table is left in the PG database
        # - try to insert new data to mongodb and check
        # if its left out from the data transfer
        # this one should remove employee from the collection map
        create_and_populate_company_mdb()
        create_and_populate_employee_mdb()

        cursor = pg.conn.cursor()
        cursor.execute(query["table_drop_company"])
        cursor.execute(query["table_drop_employee"])
        cursor.execute(query["table_drop_purr_cm"])

        # create table for CM in the database
        cm.create_table(pg, coll_conf)
        ex = extractor.Extractor(
            pg,
            mongo,
            mock.setup_pg,
            mock.settings,
            mock.coll_config_company_employee
        )

        # # changes extractor's collection definition for every collection
        # # and transfers
        coll_map_old = mock.coll_config_db_company_employee
        coll_map_new = mock.coll_config_db
        ex.table_track(coll_map_old, coll_map_new)

        mocked = mock.coll_config
        if (len(ex.coll_def) != len(mocked)):
            assert False

        for k, v in ex.coll_def.items():
            if v != mocked[k]:
                print(k)
                print(v[":columns"])
                print(mocked[k][":columns"])
                if v[":columns"] != mocked[k][":columns"]:
                    assert False

        cursor.close()
        del ex
        assert True

    def test_update_coll_map(self):
        assert True == True

    def test_transfer(self):
        assert True == True

    def test_transfer_coll(self):
        # TODO: check
        # transfer one collection

        # cmd = "DROP TABLE IF EXISTS %s;" % rel_name_company
        # cursor = pg.conn.cursor()
        # cursor.execute(cmd)

        # ex = extractor.Extractor(pg, mongo, setup_pg, settings, coll_conf)
        # ex.transfer_coll(coll_name_company)

        # cmd = "SELECT count(*) from %s;" % rel_name_company
        # cnt_pg = pg.execute_cmd_with_fetch(cmd)
        # cnt_mongo = mongo[coll_name_company].count()
        # print(cnt_pg[0][0], cnt_mongo)
        # cursor.close()
        # assert cnt_pg[0][0] == cnt_mongo
        assert True == True

    def test_insert_multiple(self):
        assert True == True

    def test_update_multiple(self):
        assert True == True

    def test_prepare_attr_details(self):
        assert True == True

    def test_adjust_columns(self):
        assert True == True

    # def test_prepare_attr_details_with_extra_props(self):
    #     ex.include_extra_props = True
    #     attribute_details = ex.prepare_attr_details(
    #         attrs_conf, attrs_mdb, types_conf, type_extra_props_pg)
    #     import pprint
    #     pprint.pprint(attribute_details)
    #     res = {
    #         '_id': {'name_conf': 'id', 'type_conf': 'text', 'value': None},
    #         'active': {'name_conf': 'active', 'type_conf': 'boolean', 'value': None},
    #         'domains': {'name_conf': 'domains', 'type_conf': 'jsonb', 'value': None},
    #         'extraProps': {'name_conf': '_extra_props', 'type_conf': 'jsonb', 'value': None},
    #         'signupCode': {'name_conf': 'signup_code', 'type_conf': 'text', 'value': None}
    #     }
    #     assert res == attribute_details

    # def test_prepare_attr_details_without_extra_props(self):
    #     attrs_conf = ['id', 'active', 'domains', 'signup_code']
    #     attrs_mdb = ['_id', 'active', 'domains', 'signupCode']
    #     types_conf = ['text', 'boolean', 'jsonb', 'text']
    #     type_extra_props_pg = None
    #     ex = extractor.Extractor(pg, mongo, setup_pg, settings, coll_conf)
    #     ex.include_extra_props = False
    #     attribute_details = ex.prepare_attr_details(
    #         attrs_conf, attrs_mdb, types_conf, type_extra_props_pg)
    #     import pprint
    #     pprint.pprint(attribute_details)
    #     res = {
    #         '_id': {'name_conf': 'id', 'type_conf': 'text', 'value': None},
    #         'active': {'name_conf': 'active', 'type_conf': 'boolean', 'value': None},
    #         'domains': {'name_conf': 'domains', 'type_conf': 'jsonb', 'value': None},
    #         'signupCode': {'name_conf': 'signup_code', 'type_conf': 'text', 'value': None}
    #     }
    #     assert res == attribute_details

    def test_adjust_columns(self):
        assert 1 == 1
