from etl.extract import collection
from etl.extract import tailer
import unittest
from tests.meta import mock
from bson import ObjectId


class TestTailer(unittest.TestCase):
    def test_prepare_docs_for_update_one(self):
        docs = [{'coll_name': 'Company',
                 'db_name': 'test_purrito',
                 'o': {'$set': {'active': True, 'name': True}},
                 'o2': {'_id': ObjectId('5caf5998a54d758375bd9928')},
                 'op': 'u'}]
        mocked = [{'_id': '5caf5998a54d758375bd9928',
                   'active': True, 'name': True}]
        (docs_useful, merged) = tailer.prepare_docs_for_update(docs)
        assert mocked == docs_useful and merged is False

    def test_prepare_docs_for_update_multiple_merged(self):
        docs = [{'coll_name': 'Company',
                 'db_name': 'test_purrito',
                 'o': {'$set': {'active': True, 'name': True}},
                 'o2': {'_id': ObjectId('5caf5998a54d758375bd9928')},
                 'op': 'u'},
                {'coll_name': 'Company',
                 'db_name': 'test_purrito',
                 'o': {'$set': {'name': 'stuffy'}, '$unset': {'active': True}},
                 'o2': {'_id': ObjectId('5caf5998a54d758375bd9928')},
                 'op': 'u'}]
        mocked = [{'_id': '5caf5998a54d758375bd9928',
                   'active': '$unset', 'name': 'stuffy'}]
        (docs_useful, merged) = tailer.prepare_docs_for_update(docs)

        assert mocked == docs_useful and merged is True
