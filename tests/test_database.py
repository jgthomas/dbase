import unittest
import os
from dbase import Database


class DatabaseTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = Database("unit_test.db")
        cls.rows = [
            {"id": 1, "val": "foo"},
            {"id": 2, "val": "bar"},
            {"id": 3, "val": "baz"}
        ]

    def setUp(self):
        self.db.execute("""CREATE TABLE test(
                id  INTEGER PRIMARY KEY, 
                val TEXT)""")

    def test_select_returns_list_of_row_as_dicts(self):
        self.assertEqual(self.db.execute("SELECT * FROM test"), [])
        for row in self.rows:
            self.db.execute("INSERT INTO test(val) VALUES(?)", row["val"])
        self.assertEqual(self.db.execute(
            "SELECT * FROM test"), self.rows)

    def test_insert_returns_last_row_id(self):
        self.assertEqual(self.db.execute(
            "INSERT INTO test(val) VALUES('one')"), 1)
        self.assertEqual(self.db.execute(
            "INSERT INTO test(val) VALUES('two')"), 2)

    def test_update_returns_affected_row_count(self):
        self.assertEqual(self.db.execute("SELECT * FROM test"), [])
        for row in self.rows:
            self.db.execute("INSERT INTO test(val) VALUES(?)", row["val"])
        self.assertEqual(self.db.execute(
            "UPDATE test SET val=? WHERE id>?", 'puff', 1), 2)

    def test_delete_returns_affected_row_count(self):
        self.assertEqual(self.db.execute("SELECT * FROM test"), [])
        for row in self.rows:
            self.db.execute("INSERT INTO test(val) VALUES(?)", row["val"])
        self.assertEqual(self.db.execute(
            "DELETE FROM test WHERE id>?", 1), 2)

    def test_exists_table(self):
        """ Return True if the table exists, else False. """
        self.assertEqual(self.db.exists_table("test"), True)
        self.assertEqual(self.db.exists_table("not_test"), False)

    def tearDown(self):
        self.db.execute("DROP TABLE test")

    @classmethod
    def tearDownClass(cls):
        cls.db.execute("DROP TABLE IF EXISTS test")
        os.remove("unit_test.db")


if __name__ == '__main__':
    unittest.main()
