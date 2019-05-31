from etl.extract import collection
import unittest
from tests.meta import mock

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


class TestCollections(unittest.TestCase):
    def test_check(self):
        reset_dataset_mdb()
        colls_in_db = collection.check(mongo, colls)
        print(colls)
        print(colls_in_db)
        assert colls == colls_in_db

    def test_get_by_name(self):
        reset_dataset_mdb()
        coll_name = "Company"
        docs = collection.get_by_name(mongo, coll_name)
        result = []
        for doc in docs:
            result.append({
                "active": doc["active"],
                "signupCode": doc["signupCode"],
                "domains": doc["domains"]
            })
        mocked = []
        for doc in mock.data_mdb_company:
            mocked.append({
                "active": doc["active"],
                "signupCode": doc["signupCode"],
                "domains": doc["domains"]
            })
        assert result == mocked

    def test_get_docs_for_type_check(self):
        reset_dataset_mdb()
        coll_name = "Employee"
        nr_of_docs = 2
        docs = collection.get_docs_for_type_check(
            mongo, coll_name, nr_of_docs)
        result = []
        for doc in docs:
            result.append(doc)
        length_equal = len(result) == nr_of_docs
        order_desc = result[0]['lastName'] == 'Hound'
        if length_equal is False:
            print("Number of requested documents differs: %d instead of %d" %
                  (len(result), nr_of_docs))
            assert False
        if order_desc is False:
            print("Order is not descending: %s", result)
            assert False
        assert True

    def test_get_by_name_reduced(self):
        reset_dataset_mdb()
        coll_name = "Employee"
        nr_of_docs = 2
        fields = ["firstName", "hair"]
        result = []
        docs = collection.get_by_name_reduced(
            mongo, coll_name, fields, nr_of_docs)

        for doc in docs:
            # leave out ObjectId
            result.append(
                {
                    "firstName": doc["firstName"],
                    "hair": doc["hair"]
                }
            )

        mocked = []
        for doc in mock.data_mdb_employee[::-1]:
            mocked.append(
                {
                    "firstName": doc["firstName"],
                    "hair": doc["hair"]
                }

            )
        print(result)
        print(mocked)
        if len(result) != len(mocked):
            assert False
        elif result != mocked:
            assert False
        else:
            return True

    def test_get_all(self):
        reset_dataset_mdb()
        mocked = mock.coll_names
        result = collection.get_all(mongo)
        assert mocked == result

    def test_get_doc_by_id(self):
        reset_dataset_mdb()
        coll_name = "Employee"
        obj = {"thing": 123}
        doc = mongo[coll_name].insert_one(obj)

        oid = str(doc.inserted_id)
        obj["_id"] = doc.inserted_id
        result = collection.get_doc_by_id(mongo, coll_name, oid)
        print(obj)
        print(result)
        assert result == obj
