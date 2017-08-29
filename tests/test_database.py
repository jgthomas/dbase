import unittest
import os
from dbase import Database


class DatabaseTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = Database("unit_test.db")
        cls.rows = [
            {"id": 1, "val": "dog"},
            {"id": 2, "val": "cat"},
            {"id": 3, "val": "man"},
            {"id": 4, "val": None}
        ]

    def setUp(self):
        self.db.execute("""CREATE TABLE test(
                id  INTEGER PRIMARY KEY, 
                val TEXT)""")
        self.db.execute("""CREATE TABLE empty_test(
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
        for row in self.rows:
            self.db.execute("INSERT INTO test(val) VALUES(?)", row["val"])
        self.assertEqual(self.db.execute(
            "UPDATE test SET val=? WHERE id>?", 'puff', 1), 3)

    def test_delete_returns_affected_row_count(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(val) VALUES(?)", row["val"])
        self.assertEqual(self.db.execute(
            "DELETE FROM test WHERE id>?", 1), 3)

    def test_delete_all(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(val) VALUES(?)", row["val"])
        self.assertEqual(self.db.execute(
            "DELETE FROM test"), 4)

    def test_exists_table(self):
        """ Return True if the table exists, else False. """
        self.assertEqual(self.db.exists_table("test"), True)
        self.assertEqual(self.db.exists_table("not_test"), False)

    def test_column_names(self):
        columns = ["id", "val"]
        not_columns = None
        for row in self.rows:
            self.db.execute("INSERT INTO test(val) VALUES(?)", row["val"])
        self.assertEqual(self.db.column_names("test"), columns)
        self.assertEqual(self.db.column_names("not_test"), not_columns)
        self.assertEqual(self.db.column_names("empty_test"), columns)

    def test_column_totals(self):
        totals = {"id": 4, "val": 3}
        not_totals = None
        empty_totals = {"id": 0, "val": 0}
        for row in self.rows:
            self.db.execute("INSERT INTO test(val) VALUES(?)", row["val"])
        self.assertEqual(self.db.column_totals("test"), totals)
        self.assertEqual(self.db.column_totals("not_test"), not_totals)
        self.assertEqual(self.db.column_totals("empty_test"), empty_totals)

    def test_row_total(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(val) VALUES(?)", row["val"])
        self.assertEqual(self.db.row_total("test"), 4)
        self.assertEqual(self.db.row_total("not_test"), None)
        self.assertEqual(self.db.row_total("empty_test"), 0)

    def tearDown(self):
        self.db.execute("DROP TABLE test")
        self.db.execute("DROP TABLE empty_test")

    @classmethod
    def tearDownClass(cls):
        cls.db.execute("DROP TABLE IF EXISTS test")
        cls.db.execute("DROP TABLE IF EXISTS empty_test")
        os.remove("unit_test.db")


if __name__ == '__main__':
    unittest.main()
