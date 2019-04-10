import psycopg2
import pymongo
from etl.extract import extractor, extractor
from etl.extract import collection
from etl.transform import relation
import pytest
import unittest
from bson import ObjectId
from tests.meta import mock
from etl.extract import collection_map as cm
import copy

mongo = mock.mongo
colls = ["Company", "Employee"]


def reset_dataset_mdb():
    # TODO
    print("resets collection company and employee")
    mongo.drop_collection(mock.coll_name_company)
    mongo.drop_collection(mock.coll_name_employee)
    docs = mock.data_mdb_company
    mongo[mock.coll_name_company].insert_many(docs)
    docs = mock.data_mdb_employee
    mongo[mock.coll_name_employee].insert_many(docs)


class TestRelation(unittest.TestCase):
    def test_init_values(self):
        expected = mock.attr_details
        attrs = mock.attr_details_with_values
        result = relation.init_values(attrs)
        print(expected)
        print(result)
        assert expected == result

    def test_set_values_no_ex_props(self):
        reset_dataset_mdb()
        attrs = mock.attr_details
        coll_name = "Company"
        doc = mongo[coll_name].find_one({"signupCode": "uPsYdUpSy123"})
        result_attr_details = relation.set_values(attrs, doc)
        result_reduced = {}
        for k, v in result_attr_details.items():
            result_reduced[k] = str(v['value'])
        result_mocked = {}
        for k, v in attrs.items():
            result_mocked[k] = str(v['value'])
        print(result_mocked)
        print(result_reduced)
        assert result_mocked == result_reduced

    # def test_set_values_ex_props(self):
    # def test_prepare_row_for_insert(self):
        # TODO
    def test_is_schema_changed_no_change(self):
        attrs_pg = mock.attrs_company
        types_pg = mock.types_company
        attrs_cm = copy.deepcopy(mock.attrs_company)
        types_cm = copy.deepcopy(mock.types_company)
        result = relation.is_schema_changed(
            attrs_pg, types_pg, attrs_cm, types_cm)
        print(attrs_pg, types_pg, attrs_cm, types_cm)
        assert result == False

    def test_is_schema_changed_cm_new_column(self):
        attrs_pg = mock.attrs_company
        types_pg = mock.types_company
        attrs_cm = copy.deepcopy(mock.attrs_company)
        types_cm = copy.deepcopy(mock.types_company)
        attrs_cm.append("new_column")
        types_cm.append("TEXT")
        result = relation.is_schema_changed(
            attrs_pg, types_pg, attrs_cm, types_cm)
        print(attrs_pg, types_pg, attrs_cm, types_cm)
        assert result == True

    def test_is_schema_changed_pg_new_column(self):
        attrs_cm = mock.attrs_company
        types_cm = mock.types_company
        attrs_pg = copy.deepcopy(mock.attrs_company)
        types_pg = copy.deepcopy(mock.types_company)
        attrs_cm.append("new_column")
        types_cm.append("TEXT")
        result = relation.is_schema_changed(
            attrs_pg, types_pg, attrs_cm, types_cm)
        print(attrs_pg, types_pg, attrs_cm, types_cm)
        assert result == True

    def test_is_schema_changed_cm_removed_column(self):
        attrs_pg = mock.attrs_company
        types_pg = mock.types_company
        attrs_cm = copy.deepcopy(mock.attrs_company)
        types_cm = copy.deepcopy(mock.types_company)
        attrs_cm.pop()
        types_cm.pop()
        result = relation.is_schema_changed(
            attrs_pg, types_pg, attrs_cm, types_cm)
        print(attrs_pg, types_pg, attrs_cm, types_cm)
        assert result == True

    def test_is_schema_changed_pg_removed_column(self):
        attrs_cm = mock.attrs_company
        types_cm = mock.types_company
        attrs_pg = copy.deepcopy(mock.attrs_company)
        types_pg = copy.deepcopy(mock.types_company)
        attrs_pg.pop()
        types_pg.pop()
        result = relation.is_schema_changed(
            attrs_pg, types_pg, attrs_cm, types_cm)
        print(attrs_pg, types_pg, attrs_cm, types_cm)
        assert result == True

    def test_is_schema_changed_pg_updated_name(self):
        attrs_cm = mock.attrs_company
        types_cm = mock.types_company
        attrs_pg = copy.deepcopy(mock.attrs_company)
        types_pg = copy.deepcopy(mock.types_company)
        types_pg[len(types_pg)-1] = "updated_colname"
        result = relation.is_schema_changed(
            attrs_pg, types_pg, attrs_cm, types_cm)
        print(attrs_pg, types_pg, attrs_cm, types_cm)
        assert result == True

    def test_is_schema_changed_pg_updated_type(self):
        attrs_cm = mock.attrs_company
        types_cm = mock.types_company
        attrs_pg = copy.deepcopy(mock.attrs_company)
        types_pg = copy.deepcopy(mock.types_company)
        types_pg[len(types_pg)-1] = "DOUBLE PRECISION"
        result = relation.is_schema_changed(
            attrs_pg, types_pg, attrs_cm, types_cm)
        print(attrs_pg, types_pg, attrs_cm, types_cm)
        assert result == True
