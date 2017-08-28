import unittest
import os
from dbase import Database


class DatabaseTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = Database("unit_test.db")

    def setUp(self):
        self.db.execute("""CREATE TABLE test(
                id  INTEGER PRIMARY KEY, 
                val TEXT)""")

    def test_insert_returns_last_row_id(self):
        """ INSERT returns id of last affected row. """
        self.assertEqual(self.db.execute(
            "INSERT INTO test(val) VALUES('one')"), 1)
        self.assertEqual(self.db.execute(
            "INSERT INTO test(val) VALUES('two')"), 2)

    def test_select_all(self):
        """ SELECT returns list, with each row as a dictionary. """
        self.assertEqual(self.db.execute("SELECT * FROM test"), [])

        rows = [
            {"id": 1, "val": "foo"},
            {"id": 2, "val": "bar"},
            {"id": 3, "val": "baz"}
        ]

        for row in rows:
            self.db.execute("INSERT INTO test(val) VALUES(?)", 
                            row["val"])
        self.assertEqual(self.db.execute("SELECT * FROM test"), rows)

    def tearDown(self):
        self.db.execute("DROP TABLE test")

    @classmethod
    def tearDownClass(cls):
        cls.db.execute("DROP TABLE IF EXISTS test")
        os.remove("unit_test.db")


if __name__ == '__main__':
    unittest.main()
