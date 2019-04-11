from etl.transform import unnester
import unittest
import datetime
from bson.json_util import ObjectId


class TestUnnester(unittest.TestCase):
    def test_cast_number_text_to_double(self):
      value_old = "2"
      column_type = "double precision"
      value_new = unnester.cast(value_old, column_type)
      assert value_new == 2

    def test_cast_text_text_to_double(self):
      value_old = "xyz"
      column_type = "double precision"
      value_new = unnester.cast(value_old, column_type)
      assert value_new == 'undefined'

    def test_cast_jsonb_to_double_undefined(self):
      value_old = {"xyz":"oh no"}
      column_type = "double precision"
      value_new = unnester.cast(value_old, column_type)
      assert value_new == 'undefined'

    def test_cast_jsonb_to_bool(self):
      value_old = {"xyz":"oh no"}
      column_type = "boolean"
      value_new = unnester.cast(value_old, column_type)
      assert value_new == 'undefined'

    def test_cast_jsonb_to_bool(self):
      value_old = {}
      column_type = "boolean"
      value_new = unnester.cast(value_old, column_type)
      assert value_new is False
    
    def test_cast_jsonb_to_timestamp(self):
      # TODO check again
      value_old = {}
      column_type = "timestamp"
      value_new = unnester.cast(value_old, column_type)
      assert value_new == value_old