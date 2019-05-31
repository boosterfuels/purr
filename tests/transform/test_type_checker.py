from etl.transform import type_checker as tc
import unittest
import datetime
from bson.json_util import ObjectId


class TestCollections(unittest.TestCase):
    def test_get_type_pg_object_id(self):
        item_old = ObjectId('5caf1643a54d750a907c07ef')
        (item_new, type_pg) = tc.get_type_pg(item_old)
        item_unchanged = item_new == item_old
        type_text = type_pg == 'text'
        print(item_unchanged, type_text)
        assert item_unchanged == type_text

    def test_get_type_pg_none(self):
        item_old = None
        (item_new, type_pg) = tc.get_type_pg(item_old)
        item_changed = item_new == 'null'
        type_text = type_pg is None
        print(item_changed, type_text)
        assert item_changed == type_text

    def test_get_type_pg_datetime(self):
        item_old = datetime.datetime.utcnow()
        (item_new, type_pg) = tc.get_type_pg(item_old)
        item_unchanged = item_new == item_old
        type_text = type_pg == 'timestamp'
        assert item_unchanged == type_text
