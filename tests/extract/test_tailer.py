from etl.extract import collection
from etl.extract import tailer
from etl.extract import extractor
from etl.extract import transfer_info
import unittest
from tests.meta import mock
from bson import ObjectId
import datetime


mongo = mock.mongo
pg = mock.pg
query = mock.query
ex = extractor.Extractor(pg, mongo, mock.setup_pg,
                         mock.settings, mock.coll_config_company_employee)

settings_company = {
    ':columns':
    [
        {':source': '_id', ':type': 'TEXT', 'id': None},
        {':source': 'active', ':type': 'BOOLEAN', 'active': None},
        {':source': 'domains', ':type': 'JSONB', 'domains': None},
        {':source': 'signupCode', ':type': 'TEXT', 'signup_code': None}
    ],
    ':meta': {':extra_props': 'JSONB', ':table': 'company'}
}


def create_and_populate_company_pg():
    cursor = pg.conn.cursor()
    cursor.execute(query["table_drop_company"])
    cursor.execute(query["table_drop_employee"])
    query_company = """CREATE TABLE %s(%s, PRIMARY KEY (id));""" % (
        mock.rel_name_company, ', '.join(mock.attrs_types_company))

    cursor.execute(query_company)

    query_employee = """CREATE TABLE %s(%s, PRIMARY KEY (id));""" % (
        mock.rel_name_employee, ', '.join(mock.attrs_types_employee))

    cursor.execute(query_employee)
    cursor.close()


def reset_dataset():
    data_set_employee = [
        {
            "_id": "1",
            "firstName": "John",
            "lastName": "Snow",
            "hair": "black"
        },
        {
            "_id": "2",
            "firstName": "Arya",
            "lastName": "Start",
            "hair": "brown"
        }
    ]

    data_set_company = [
        {
            "_id": "1",
            "active": True,
            "signupCode": "uPsYdUpSy123",
            "domains": [
                "southpark.com"
            ]
        },
        {
            "_id": "2",
            "active": True,
            "signupCode": "node",
            "domains": [
                "amazon.com"
            ]
        }]

    mongo.drop_collection("Company")
    mongo.drop_collection("Employee")
    mongo["Company"].insert_many(data_set_company)
    mongo["Employee"].insert_many(data_set_employee)


class TestTailer(unittest.TestCase):
    def test_prepare_docs_for_update_one(self):
        docs = [{'coll_name': 'Company',
                 'db_name': 'test_purrito',
                 'o': {'$set': {'active': True, 'name': True}},
                 'o2': {'_id': ObjectId('5caf5998a54d758375bd9928')},
                 'op': 'u'}]
        mocked = [{'_id': '5caf5998a54d758375bd9928',
                   'active': True, 'name': True}]

        (docs_useful, merged) = tailer.prepare_docs_for_update(
            settings_company, docs)
        assert mocked == docs_useful and merged is False

    def test_prepare_docs_for_update_one_no_set_no_unset(self):
        docs = [{'coll_name': 'Company',
                 'db_name': 'test_purrito',
                 'o': {
                     '_id': ObjectId('5caf5998a54d758375bd9928'),
                     'active': True,
                     'name': True
                 },
                 'o2': {'_id': ObjectId('5caf5998a54d758375bd9928')},
                 'op': 'u'}]
        mocked = [{'_id': '5caf5998a54d758375bd9928',
                   'active': True, 'name': True, 'domains': '$unset', 'signupCode': '$unset'}]

        (docs_useful, merged) = tailer.prepare_docs_for_update(
            settings_company, docs)

        assert mocked == docs_useful and merged is False

    def test_prepare_docs_for_update_one_no_set_no_unset_all_nulls(self):
        docs = [{'coll_name': "Company",
                 'db_name': 'test_purrito',
                 'o': {
                     '_id': ObjectId('5caf5998a54d758375bd9928'),
                     'active': None
                 },
                 'o2': {'_id': ObjectId('5caf5998a54d758375bd9928')},
                 'op': 'u'}]
        mocked = [{
            '_id': '5caf5998a54d758375bd9928',
            'active': '$unset',
            'domains': '$unset',
            'signupCode': '$unset'
        }]
        (docs_useful, merged) = tailer.prepare_docs_for_update(
            settings_company, docs)
        assert mocked == docs_useful and merged is False

    def test_prepare_docs_for_update_multiple_merged_mixed(self):
        """
        This test covers the case when one document of a collection
        is updated directly through the IDE (e.g. Studio3T) and
        another is updated using an update query:
        coll.update({}, {"$set"}, {"$unset"})
        """
        docs = [{'coll_name': 'Company',
                 'db_name': 'test_purrito',
                 'o': {
                     '_id': ObjectId('5caf5998a54d758375bd9928'),
                     'active': True,
                     'name': True
                 },
                 'o2': {'_id': ObjectId('5caf5998a54d758375bd9928')},
                 'op': 'u'},

                {'coll_name': 'Company',
                 'db_name': 'test_purrito',
                 'o': {'$set': {'name': 'stuffy'}, '$unset': {'active': True}},
                 'o2': {'_id': ObjectId('5caf5998a54d758375bd9929')},
                 'op': 'u'}]

        mocked = [{'_id': '5caf5998a54d758375bd9928',
                   'active': True, 'name': True, 'domains': '$unset', 'signupCode': '$unset'},
                  {'_id': '5caf5998a54d758375bd9929',
                   'active': '$unset', 'name': 'stuffy'}]

        (docs_useful, merged) = tailer.prepare_docs_for_update(
            settings_company, docs)

        assert mocked == docs_useful and merged is False

    def test_prepare_docs_for_update_multiple_merged(self):
        docs = [{'coll_name': 'Company',
                 'db_name': 'test_purrito',
                 'o': {'$set': {'active': True, 'name': True}},
                 'o2': {'_id': ObjectId('5caf5998a54d758375bd9929')},
                 'op': 'u'},
                {'coll_name': 'Company',
                 'db_name': 'test_purrito',
                 'o': {'$set': {'name': 'stuffy'}, '$unset': {'active': True}},
                 'o2': {'_id': ObjectId('5caf5998a54d758375bd9929')},
                 'op': 'u'}]
        mocked = [{'_id': '5caf5998a54d758375bd9929',
                   'active': '$unset', 'name': 'stuffy'}]

        collection_settings = mock.coll_config
        collection_name = "Company"

        (docs_useful, merged) = tailer.prepare_docs_for_update(
            collection_settings[collection_name], docs)

        assert mocked == docs_useful and merged is True

    def test_handle_multiple(self):
        transfer_info.create_oplog_table(pg, 'public')
        reset_dataset()
        oplog_entries = mock.oplog_entries_update
        create_and_populate_company_pg()

        colls = ["Company", "Employee"]

        ex.transfer(colls)

        cursor = pg.conn.cursor()
        cursor.execute("SELECT * FROM public.employee where id = '1';")
        res = cursor.fetchall()

        t = tailer.Tailer(pg,
                          mongo,
                          mock.setup_pg,
                          mock.settings,
                          mock.coll_config_company_employee)
        ts = datetime.datetime.utcnow()-datetime.timedelta(minutes=2)

        t.handle_multiple(oplog_entries, ts)

        cursor = pg.conn.cursor()
        cursor.execute(
            "SELECT id, active, signup_code, domains FROM public.company WHERE id = '2';")
        res = cursor.fetchone()

        mocked_company = ("2", True, "node", ["dragonglass.org"])

        if res != mocked_company:
            for i in range(len(mocked_company)):
                if mocked_company[i] != res[i]:
                    assert False

        cursor.execute(
            "SELECT id, first_name, last_name, hair FROM public.employee WHERE id='1';")
        res = cursor.fetchone()

        mocked_janos = ('1', "Janos", None, None)

        if res != mocked_janos:
            for i in range(len(mocked_janos)):
                if mocked_janos[i] != res[i]:
                    assert False

        cursor.execute(
            "SELECT id, first_name, last_name, hair FROM public.employee WHERE id='2';")
        res = cursor.fetchone()
        cursor.close()

        mocked_arja = ("2", "Arja", "Stark", "blonde")

        if res != mocked_arja:
            for i in range(len(mocked_arja)):
                if mocked_arja[i] != res[i]:
                    assert False

        assert True

    def test_log_tailed_docs_one(self):
        transfer_info.create_oplog_table(pg)

        ids_log = ['58a32cfda51183070034909b']
        docs = [{
            '_id': '58a32cfda51183070034909b',
            'updatedAt': datetime.datetime(2019, 4, 29, 11, 26, 50, 703000)
        }]
        table_name = 'some_relation'
        oper = 'u'
        merged = True
        cursor = pg.conn.cursor()
        schema = 'public'
        tailer.log_tailed_docs(pg, schema, docs,
                               ids_log, table_name, oper,
                               merged)
        cursor.execute(
            """SELECT operation,
            relation, obj_id, merged, document FROM purr_oplog;""")
        res = cursor.fetchone()
        cursor.close()
        mock = tuple(
            ['u',
             'some_relation',
             '58a32cfda51183070034909b',
             True,
             "{'_id': '58a32cfda51183070034909b', 'updatedAt': datetime.datetime(2019, 4, 29, 11, 26, 50, 703000)}"
             ])
        assert mock == res

    def test_log_tailed_docs_multiple(self):
        transfer_info.create_oplog_table(pg)

        ids_log = ['58a32cfda51183070034909b', '58a32cfda51183070034909c']
        docs = [{
            '_id': '58a32cfda51183070034909b',
            'updatedAt': datetime.datetime(2019, 4, 29, 11, 26, 50, 703000)
        }, {
            '_id': '58a32cfda51183070034909c',
            'updatedAt': datetime.datetime(2019, 5, 21, 2, 24, 24, 5)
        }]
        table_name = 'some_relation'
        oper = 'u'
        merged = False
        cursor = pg.conn.cursor()
        schema = 'public'
        tailer.log_tailed_docs(pg, schema, docs,
                               ids_log, table_name, oper,
                               merged)
        cursor.execute(
            """SELECT operation,
            relation, obj_id, merged, document FROM purr_oplog order by id;""")
        res = cursor.fetchall()
        mock = [
            tuple(
                ['u',
                 'some_relation',
                 '58a32cfda51183070034909b',
                 False,
                 "{'_id': '58a32cfda51183070034909b', 'updatedAt': datetime.datetime(2019, 4, 29, 11, 26, 50, 703000)}"
                 ]),
            tuple(
                ['u',
                 'some_relation',
                 '58a32cfda51183070034909c',
                 False,
                 "{'_id': '58a32cfda51183070034909c', 'updatedAt': datetime.datetime(2019, 5, 21, 2, 24, 24, 5)}"
                 ])]
        cursor.close()
        assert mock == res
