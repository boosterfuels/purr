from etl.load import table, constraint
import unittest
from tests.meta import mock

pg = mock.pg
schema = mock.schema
query = mock.query
rel_name = 'test_constraint'
attrs = mock.attrs_company
types = mock.types_company


def reset_dataset_pg():
    cursor = pg.conn.cursor()
    cursor.execute("drop table if exists %s;" % rel_name)
    cursor.close()


class TestConstraint(unittest.TestCase):
    def test_add_pk(self):
        reset_dataset_pg()
        cursor = pg.conn.cursor()
        cmd_create_test = """
        CREATE TABLE %s(
            id INTEGER, 
            column_pretty TEXT, 
            column_ugly TEXT
        );
        """ % (rel_name)
        cursor.execute(cmd_create_test)
        attr_for_pk = 'id'
        constraint.add_pk(pg, schema, rel_name, 'id')

        cmd_check_constraint = """
            SELECT constraint_name
            FROM information_schema.constraint_column_usage
            WHERE table_name='%s'
            AND column_name='%s';
        """ % (rel_name, attr_for_pk)
        cursor.execute(cmd_check_constraint)
        res = cursor.fetchone()
        constraint_name_default = 'test_constraint_pkey'
        assert res[0] == constraint_name_default