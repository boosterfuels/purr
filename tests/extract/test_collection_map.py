import unittest
from bson import ObjectId
from tests.meta import mock
from etl.extract import collection_map as cm

mongo = mock.mongo
colls = ["Company", "Employee"]


def reset_dataset_mdb():
    print("resets collection company and employee")
    mongo.drop_collection(mock.coll_name_company)
    mongo.drop_collection(mock.coll_name_employee)
    docs = mock.data_mdb_company
    mongo[mock.coll_name_company].insert_many(docs)
    docs = mock.data_mdb_employee
    mongo[mock.coll_name_employee].insert_many(docs)


class TestCollections(unittest.TestCase):
    def test_get_types(self):
        reset_dataset_mdb()
        docs = [
            {'_id': ObjectId('5caf1062a54d75f29d118eba'),
             'firstName': 'The',
             'lastName': 'Hound',
             'hair': 'brown'},
            {'_id': ObjectId('5caf1062a54d75f29d118eb9'),
             'firstName': 'Little',
             'lastName': 'Finger',
             'hair': 'dark brown'},
            {'_id': ObjectId('5caf1062a54d75f29d118eb8'),
             'firstName': 'Sansa',
             'lastName': 'Stark',
             'hair': 'ginger'},
            {'_id': ObjectId('5caf1062a54d75f29d118eb7'),
             'firstName': 'Arya',
             'lastName': 'Start',
             'hair': 'brown'},
            {'_id': ObjectId('5caf1062a54d75f29d118eb6'),
             'firstName': 'John',
             'lastName': 'Snow',
             'hair': 'black'}
        ]
        types = cm.get_types(docs)

        mocked_types = {
            '_id': {'text': 5},
            'firstName': {'text': 5},
            'lastName': {'text': 5},
            'hair': {'text': 5}
        }

        assert mocked_types == types
