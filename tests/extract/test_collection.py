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


def create_dataset_mdb():
    # TODO
    print("creates collection company and employee")
    mongo.drop_collection(coll_name_company)
    mongo.drop_collection(coll_name_employee)
    docs = mock.data_mdb_company
    mongo[coll_name_company].insert_many(docs)
    docs = mock.data_mdb_employee
    mongo[coll_name_employee].insert_many(docs)


class TestCollections(unittest.TestCase):
    def test_check(self):
        colls_in_db = collection.check(mongo, colls)
        print(colls)
        print(colls_in_db)
        assert colls == colls_in_db

    def test_get_by_name(self):
        docs = collection.get_by_name(mongo, "Company")
        result = []
        for doc in docs:
            del doc["_id"]
            result.append(doc)
        print(mock.data_mdb_company)
        assert result == mock.data_mdb_company

    def test_get_docs_for_type_check(self):
        nr_of_docs_requested = 2
        docs = collection.get_docs_for_type_check(
            mongo, "Employee", nr_of_docs_requested)
        result = []
        for doc in docs:
            result.append(doc)
        import pprint
        length_equal = len(result) == nr_of_docs_requested
        order_desc = result[0]['lastName'] == 'Hound'
        if length_equal is False:
            print("Number of requested documents differs: %d instead of %d" %
                  (len(result), nr_of_docs_requested))
            assert False
        if order_desc is False:
            print("Order is not descending: %s", result)
            assert False
        assert True
