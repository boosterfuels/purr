import psycopg2
import pymongo
from etl.extract import extractor, extractor
from etl.load import row
from etl.transform import relation
import pytest
import unittest
from bson import ObjectId
from tests.meta import mock
from etl.extract import collection_map as cm
import copy

pg = mock.pg
query = mock.query
rel_name_company = mock.rel_name_company


def reset_dataset_pg():
    cursor = pg.conn.cursor()

    cursor.execute(query["table_drop_company"])
    cursor.execute(query["table_create_company"])

    cursor.execute(query["table_drop_employee"])
    cursor.execute(query["table_create_employee"])
    cursor.execute(
        """insert into company(id, active, domains, signup_code) values('12345', 'true', '{"domain": ["pelotonland.com"]}', 'xfLfdsFD3S')""")
    cursor.close()


class TestRow(unittest.TestCase):
    def test_insert(self):
        reset_dataset_pg()
        schema = 'public'
        table = 'employee'
        attrs = ["id", "first_name", "last_name", "hair"]
        values = ["12345", "Purr", "Rito", "orange"]
        row.insert(pg, schema, table, attrs, values)
        cmd = "select * from %s where id = '%s'" % (table, values[0])
        cursor = pg.conn.cursor()
        cursor.execute(cmd)
        res = cursor.fetchone()
        print(tuple(values))
        print(res)
        cursor.close()
        assert tuple(values) == res

    def test_insert_bulk(self):
        reset_dataset_pg()
        schema = 'public'
        table = 'employee'
        attrs = ["id", "first_name", "last_name", "hair"]
        values = [
            ["12345", "Purr", "Rito", "orange"],
            ["12346", "James", "Cat", "black and white"],
            ["12347", "Morgo", None, "black and white"],
            ["12348", "Voros", "Macska", "orange"],
            ["12349", "Szurke", "Macska", "grey"]
        ]
        row.insert_bulk(pg, schema, table, attrs, values)
        cmd = "select * from %s where id = '%s'" % (table, values[2][0])
        cursor = pg.conn.cursor()
        cursor.execute(cmd)
        res = cursor.fetchone()
        print(tuple(values[2]))
        print(res)
        cursor.close()
        assert tuple(values[2]) == res
