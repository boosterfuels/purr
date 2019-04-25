from etl.load import row, constraint
import unittest
from tests.meta import mock

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
        """insert into company(id, active, domains, signup_code)
            values(
            '12345', 'true', '{"domain": ["pelotonland.com"]}', 'xfLfdsFD3S'
            )""")
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

    def test_upsert_bulk(self):
        reset_dataset_pg()
        schema = 'public'
        table = 'employee'
        attrs = ["id", "first_name", "last_name", "hair"]
        constraint.add_pk(pg, schema, table, attrs[0])
        values = [
            ["12345", "Purr", "Rito", "orange"],
            ["12346", "James", "Cat", "black and white"],
            ["12347", "Morgo", None, "black and white"],
            ["12348", "Voros", "Macska", "orange"],
            ["12349", "Szurke", "Macska", "grey"]
        ]
        row.insert_bulk(pg, schema, table, attrs, values)
        values_new = [
            ["12346", "James", "Cat", "black and white"],
            ["12347", "Morgo", None, "black and white"],
            ["12348", "Zuta", "Macska", "yellow"],  # changed
            ["12349", "Szurke", "Cicus", "grey"],  # changed
            ["12350", "Fekete", "Cica", "black"]  # new
        ]
        row.upsert_bulk(pg, schema, table, attrs, values_new)
        cmd = "select * from %s" % table
        # cmd = "select * from %s where id = '%s'" % (table, values[5][0])
        cursor = pg.conn.cursor()
        cursor.execute(cmd)
        res = cursor.fetchall()
        mocked = [
            ["12345", "Purr", "Rito", "orange"],
            ["12346", "James", "Cat", "black and white"],
            ["12347", "Morgo", None, "black and white"],
            ["12348", "Zuta", "Macska", "yellow"],
            ["12349", "Szurke", "Cicus", "grey"],
            ["12350", "Fekete", "Cica", "black"]
        ]
        mocked = [tuple(x) for x in mocked]
        cursor.close()
        assert mocked == res

    def test_upsert_bulk_tail(self):
        reset_dataset_pg()
        schema = 'public'
        table = 'employee'
        attrs = ["id", "first_name", "last_name", "hair"]
        constraint.add_pk(pg, schema, table, attrs[0])
        values = [
            ["12345", "Purr", "Rito", "orange"],
            ["12346", "James", "Cat", "black and white"],
            ["12347", "Morgo", None, "black and white"],
            ["12348", "Voros", "Macska", "orange"],
            ["12349", "Szurke", "Macska", "grey"]
        ]
        row.insert_bulk(pg, schema, table, attrs, values)
        values_new = [
            ["12346", "James", "Cat", "black and white"],
            ["12347", "Morgo", None, "black and white"],
            ["12348", "Zuta", "Macska", "yellow"],  # changed
            ["12349", "Szurke", "Cicus", "grey"],  # changed
            ["12350", "Fekete", "Cica", "black"]  # new
        ]
        row.upsert_bulk_tail(pg, schema, table, attrs, values_new)
        cmd = "select * from %s" % table
        cursor = pg.conn.cursor()
        cursor.execute(cmd)
        res = cursor.fetchall()
        mocked = [
            ["12345", "Purr", "Rito", "orange"],
            ["12346", "James", "Cat", "black and white"],
            ["12347", "Morgo", None, "black and white"],
            ["12348", "Zuta", "Macska", "yellow"],
            ["12349", "Szurke", "Cicus", "grey"],
            ["12350", "Fekete", "Cica", "black"]
        ]
        mocked = [tuple(x) for x in mocked]
        cursor.close()
        assert mocked == res

    
    def test_upsert_bulk_tail_unset(self):
        reset_dataset_pg()
        schema = 'public'
        table = 'employee'
        attrs = ["id", "first_name", "last_name", "hair"]
        constraint.add_pk(pg, schema, table, attrs[0])
        values = [
            ["12345", "Purr", "Rito", "orange"],
            ["12346", "James", "Cat", "black and white"],
            ["12347", "Morgo", None, "black and white"],
            ["12348", "Voros", "Macska", "orange"],
            ["12349", "Szurke", "Macska", "grey"]
        ]
        row.insert_bulk(pg, schema, table, attrs, values)
        values_new = [
            ["12346", "James", "Cat", "black and white"],
            ["12347", "Morgo", None, "black and white"],
            ["12348", "Zuta", "Macska", "yellow"],  # changed
            ["12349", "Szurke", "$unset", "grey"], # unset
            ["12350", "$unset", "Cica", "$unset"] # unset
        ]
        row.upsert_bulk_tail(pg, schema, table, attrs, values_new)
        cmd = "select * from %s" % table
        cursor = pg.conn.cursor()
        cursor.execute(cmd)
        res = cursor.fetchall()
        mocked = [
            ["12345", "Purr", "Rito", "orange"],
            ["12346", "James", "Cat", "black and white"],
            ["12347", "Morgo", None, "black and white"],
            ["12348", "Zuta", "Macska", "yellow"],
            ["12349", "Szurke", None, "grey"],
            ["12350", None, "Cica", None]
        ]
        mocked = [tuple(x) for x in mocked]
        cursor.close()
        assert mocked == res