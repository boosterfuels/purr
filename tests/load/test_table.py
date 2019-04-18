from etl.load import table
import unittest
from tests.meta import mock

pg = mock.pg
schema = mock.schema
query = mock.query
rel_name = 'test'
attrs = mock.attrs_company
types = mock.types_company


def reset_dataset_pg():
    cursor = pg.conn.cursor()
    cursor.execute("drop table if exists test;")
    cursor.close()


class TestRow(unittest.TestCase):
    def test_create_attrs_and_types(self):
        reset_dataset_pg()
        table.create(pg, schema, rel_name, attrs, types)
        cursor = pg.conn.cursor()

        cursor.execute("SELECT * FROM %s.%s;" % (schema, rel_name))
        cursor.fetchall()
        print(cursor.description)
        columns = [x[0] for x in cursor.description]
        print(columns)

        cursor.execute("""
        select column_name, data_type
        from information_schema.columns
        where
            table_schema = '%s'
            and table_name = '%s';
        """ % (schema, rel_name))

        res = cursor.fetchall()
        res.sort(key=lambda tup: tup[0])
        col_names = [x[0] for x in res]
        col_types = [x[1].upper() for x in res]

        equal_attrs = (set(col_names) - set(attrs)) == set()
        equal_types = (set(col_types) - set(types)) == set()
        print(equal_attrs)
        print(equal_types)
        cursor.close()
        assert equal_attrs and equal_types

    def test_create_pk(self):
        reset_dataset_pg()
        pk = ["id"]
        table.create(pg, schema, rel_name, attrs, types)
        cursor = pg.conn.cursor()

        cursor.execute("""
            select column_name
            from information_schema.constraint_column_usage
            where
                table_schema = '%s'
                and table_name = '%s';
        """ % (schema, rel_name))

        res = cursor.fetchall()
        col_names = [x[0] for x in res]
        cursor.close()
        assert col_names == pk

    def test_create_different_pk(self):
        reset_dataset_pg()
        pk = ["signup_code"]
        table.create(pg, schema, rel_name, attrs, types, pk)
        cursor = pg.conn.cursor()

        cursor.execute("""
            select column_name
            from information_schema.constraint_column_usage
            where
                table_schema = '%s'
                and table_name = '%s';
        """ % (schema, rel_name))

        res = cursor.fetchall()
        col_names = [x[0] for x in res]
        print(col_names, pk)
        cursor.close()
        assert col_names == pk

    def test_create_multiple_pks(self):
        reset_dataset_pg()
        pk = ["id", "signup_code"]
        table.create(pg, schema, rel_name, attrs, types, pk)
        cursor = pg.conn.cursor()

        cursor.execute("""
            select column_name
            from information_schema.constraint_column_usage
            where
                table_schema = '%s'
                and table_name = '%s';
        """ % (schema, rel_name))

        res = cursor.fetchall()
        col_names = [x[0] for x in res]
        print(col_names, pk)
        cursor.close()
        assert col_names == pk
