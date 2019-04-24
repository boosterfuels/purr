from etl.transform import relation
from etl.extract import extractor
import unittest
from tests.meta import mock
from etl.extract import collection_map as cm
import copy

pg = mock.pg
mongo = mock.mongo
query = mock.query
rel_name_company = mock.rel_name_company
coll_name_company = mock.coll_name_company
coll_name_employee = mock.coll_name_employee
coll_conf_new = mock.coll_config_new
ex = extractor.Extractor(pg, mongo, mock.setup_pg,
                         mock.settings, mock.coll_config)
pg_cm_attrs = mock.pg_coll_map_attrs


def create_and_populate_company_pg():
    cursor = pg.conn.cursor()
    cursor.execute(query["table_drop_purr_cm"])
    # create table for CM in the database
    cm.create_table(pg, mock.coll_config)

    cursor.execute(query["table_drop_company"])
    cursor.execute(query["table_create_company"])

    cursor.execute(
        """insert into company(
            id, active, domains, signup_code
            ) values(
                '12345', 'true',
                '{"domain": ["pelotonland.com"]}',
                'xfLfdsFD3S')""")

    cursor.close()


def create_and_populate_company_mdb():
    # TODO
    mongo.drop_collection(coll_name_company)
    print("creates collection company")
    docs = mock.data_mdb_company
    mongo[coll_name_company].insert_many(docs)


def create_and_populate_employee_mdb():
    mongo.drop_collection(coll_name_employee)
    print("creates collection employee")
    docs = mock.data_mdb_employee
    mongo[coll_name_employee].insert_many(docs)


class TestExtractor(unittest.TestCase):

    def test_init_db(self):
        # create test databases in Mongo and Postgres
        # Postgres
        cursor = pg.conn.cursor()
        cursor.execute(query["db_exists"])
        db_exists = cursor.fetchone()
        assert db_exists[0] is True
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
        res_expected = [
            ('active', 'boolean'), ('domains', 'jsonb'),
            ('id', 'text'), ('signup_code', 'text'),
            ('updated_at', 'timestamp without time zone')
        ]

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
        conf = copy.deepcopy(mock.coll_config_db)
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

        coll_config = copy.deepcopy(mock.coll_config)
        # create table for CM in the database
        cm.create_table(pg, mock.coll_config)
        ex = extractor.Extractor(
            pg, mongo, mock.setup_pg, mock.settings, coll_config)

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

        cursor.execute(query["table_drop_purr_cm"])
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
        cm.create_table(pg, mock.coll_config)
        coll_config = copy.deepcopy(mock.coll_config_company_employee)

        ex = extractor.Extractor(
            pg,
            mongo,
            mock.setup_pg,
            mock.settings,
            coll_config
        )

        # # changes extractor's collection definition for every collection
        # # and transfers

        coll_map_old = copy.deepcopy(mock.coll_config_db_company_employee)
        coll_map_new = copy.deepcopy(mock.coll_config_db)
        ex.table_untrack(coll_map_old, coll_map_new)

        mocked = copy.deepcopy(mock.coll_config)
        if (len(ex.coll_def) != len(mocked)):
            print("NEW", ex.coll_def)
            print("OlD", mocked)
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

    def test_update_coll_map_unchanged(self):

        # collection map is not changed
        # extractor.coll_map_cur stays the same

        cursor = pg.conn.cursor()

        cursor.execute(query["table_drop_purr_cm"])

        # create table for CM in the database
        cm.create_table(pg, mock.coll_config)

        coll_config = copy.deepcopy(mock.coll_config)
        ex = extractor.Extractor(
            pg,
            mongo,
            mock.setup_pg,
            mock.settings,
            coll_config
        )
        ex.update_coll_map()
        print("OLD", ex.coll_map_cur)
        print("NEW", mock.coll_config_db)
        cursor.close()

        res = (ex.coll_map_cur == mock.coll_config_db)
        del ex
        assert res

    def test_update_coll_map_changed(self):

        # collection map is changed and
        # extractor.coll_map_cur needs to be updated

        # purr_collection_map needs old values
        cursor = pg.conn.cursor()
        cursor.execute(query["table_drop_purr_cm"])

        # create table for CM in the database
        cm.create_table(pg, mock.coll_config)

        coll_config = copy.deepcopy(mock.coll_config_company_employee)

        ex = extractor.Extractor(
            pg,
            mongo,
            mock.setup_pg,
            mock.settings,
            coll_config
        )

        # this pulls the map from the db (mock.coll_config)
        ex.update_coll_map()
        print("NEW", ex.coll_map_cur)
        print("MOCK", mock.coll_config_db_company_employee)
        # case 2: collection map is changed and
        # extractor.coll_map_cur needs to be updated
        cursor.close()

        res = (ex.coll_map_cur == mock.coll_config_db)
        del ex
        assert res

    def test_transfer(self):
        # drop/truncate table
        # create schema
        # transfers collections
        cursor = pg.conn.cursor()

        # reset CM in the database
        cursor.execute(query["table_drop_purr_cm"])
        cursor.execute(query["table_drop_company"])
        cursor.execute(query["table_drop_employee"])
        create_and_populate_company_mdb()
        cm.create_table(pg, mock.coll_config)

        # collection which will be transferred
        collection_names = mock.coll_names

        coll_config = copy.deepcopy(mock.coll_config_company_employee)
        ex = extractor.Extractor(
            pg,
            mongo,
            mock.setup_pg,
            mock.settings,
            coll_config
        )
        ex.transfer(collection_names)
        for i in range(len(mock.rel_names)):
            relation = mock.rel_names[i]
            cursor.execute("SELECT count(*) FROM %s" % (relation))
            cnt_pg = cursor.fetchone()
            cnt_mongo = mongo[collection_names[i]].count()
            if cnt_mongo != cnt_pg[0]:
                print("Postgres:", cnt_pg[0])
                print("MongoDB:", cnt_mongo)
                assert False

        cursor.close()
        del ex

        assert True

    def test_transfer_coll(self):
        # TODO: check

        cursor = pg.conn.cursor()

        # reset CM in the database
        cursor.execute(query["table_drop_purr_cm"])
        cursor.execute(query["table_drop_company"])
        cursor.execute(query["table_drop_employee"])
        create_and_populate_company_mdb()
        cm.create_table(pg, mock.coll_config)

        # collection which will be transferred
        collection = mock.coll_names[0]
        relation = mock.rel_names[0]

        coll_config = copy.deepcopy(mock.coll_config_company_employee)
        ex = extractor.Extractor(
            pg,
            mongo,
            mock.setup_pg,
            mock.settings,
            coll_config
        )
        ex.transfer_coll(collection)
        cursor.execute("SELECT count(*) FROM %s" % (relation))
        cnt_pg = cursor.fetchone()
        cnt_mongo = mongo[collection].count()
        print("Postgres:", cnt_pg[0])
        print("MongoDB:", cnt_mongo)

        cursor.close()
        del ex
        assert cnt_mongo == cnt_pg[0]

    def test_insert_multiple(self):
        # no unset
        # TODO: test when there is value in unset
        cursor = pg.conn.cursor()

        # reset CM in the database
        cursor.execute(query["table_drop_purr_cm"])
        cursor.execute(query["table_drop_company"])

        create_and_populate_company_mdb()
        cm.create_table(pg, mock.coll_config)

        coll = mock.coll_names[0]
        rel = mock.rel_names[0]
        docs = []

        for doc in mongo[coll].find():
            docs.append(doc)
        coll_config = copy.deepcopy(mock.coll_config_company_employee)
        ex = extractor.Extractor(
            pg,
            mongo,
            mock.setup_pg,
            mock.settings,
            coll_config
        )
        schema = mock.setup_pg["schema_name"]

        r = relation.Relation(
            pg,
            schema,
            rel,
            True
        )
        attrs = mock.attrs_company
        types = mock.types_company
        r.create_with_columns(attrs, types)

        ex.insert_multiple(docs, r, coll)

        print("COLLECTION", coll)
        print("DOCUMENTS", docs)

        cmd = "SELECT %s FROM %s order by id" % (", ".join(attrs[1:]), rel)
        cursor.execute(cmd)
        mocked = mock.data_pg_company_no_id
        res = cursor.fetchall()
        print("MOCKED")
        print(mocked)
        print("RESULT")
        print(res)

        cursor.close()
        del r
        del ex
        assert mocked == res

    def test_update_multiple(self):
        # no unset
        # TODO: test when there is value in unset
        cursor = pg.conn.cursor()

        # reset CM in the database
        cursor.execute(query["table_drop_purr_cm"])
        cursor.execute(query["table_drop_company"])

        create_and_populate_company_mdb()
        cm.create_table(pg, mock.coll_config)

        coll = mock.coll_names[0]
        rel = mock.rel_names[0]
        docs = []
        mock_updated = mock.data_mdb_company_updated
        for doc in mongo[coll].find():
            docs.append({
                "_id": doc["_id"],
                "active": mock_updated["active"],
                "signupCode": mock_updated["signupCode"],
                "domains": mock_updated["domains"]
            })
        coll_config = copy.deepcopy(mock.coll_config_company_employee)
        ex = extractor.Extractor(
            pg,
            mongo,
            mock.setup_pg,
            mock.settings,
            coll_config
        )
        schema = mock.setup_pg["schema_name"]

        r = relation.Relation(
            pg,
            schema,
            rel,
            True
        )
        attrs = mock.attrs_company
        types = mock.types_company
        r.create_with_columns(attrs, types)

        ex.update_multiple(docs, r, coll)

        cmd = "SELECT %s FROM %s order by id" % (", ".join(attrs[1:]), rel)
        cursor.execute(cmd)
        mocked = mock.data_pg_company_updated_no_id
        res = cursor.fetchall()
        print("MOCKED")
        print(mocked)
        print("RESULT")
        print(res)

        cursor.close()
        del r
        del ex
        assert mocked == res

    def test_prepare_attr_details_with_extra_props(self):
        coll_config = copy.deepcopy(mock.coll_config_company_employee)
        ex = extractor.Extractor(
            pg,
            mongo,
            mock.setup_pg,
            mock.settings,
            coll_config
        )
        type_extra_props_pg = 'jsonb'

        ex.include_extra_props = True
        attrs = copy.deepcopy(mock.attrs_company)
        fields = copy.deepcopy(mock.fields_company)
        types = []
        for item in mock.types_company:
            types.append(item.lower())

        attribute_details = ex.prepare_attr_details(
            attrs, fields, types, type_extra_props_pg)

        print("RESULT")
        print(attribute_details)
        print("MOCKED")
        print(mock.attr_details)
        del ex
        assert mock.attr_details == attribute_details

    def test_prepare_attr_details_without_extra_props(self):
        coll_config = copy.deepcopy(mock.coll_config_company_employee)
        ex = extractor.Extractor(
            pg,
            mongo,
            mock.setup_pg,
            mock.settings,
            coll_config
        )

        ex.include_extra_props = False
        attrs = copy.deepcopy(mock.attrs_company)
        fields = copy.deepcopy(mock.fields_company)
        types = []
        for item in mock.types_company:
            types.append(item.lower())

        attribute_details = ex.prepare_attr_details(
            attrs, fields, types)

        print("RESULT")
        print(attribute_details)
        print("MOCKED")
        print(mock.attr_details)
        del ex
        mocked = copy.deepcopy(mock.attr_details)
        del mocked["extraProps"]

        assert mocked == attribute_details

    # def test_adjust_columns(self):
    #     assert False

    # def test_handle_failed_type_update(self):
    #     # self.attrs_details changed
    #     assert False

    # def test_add_extra_props(self):
    #     assert False
