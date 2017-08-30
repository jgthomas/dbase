import unittest
import os
import csv
import io
import sys
from dbase import Database


class DatabaseTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = Database("unit_test.db")
        cls.rows = [
                {"id": 1, "val": "dog", "age": 10},
                {"id": 2, "val": "cat", "age": 20},
                {"id": 3, "val": "man", "age": 30},
                {"id": 4, "val": "rat", "age": None}
        ]

    def setUp(self):
        self.db.execute("""CREATE TABLE test(
                id  INTEGER PRIMARY KEY, 
                val TEXT,
                age INTEGER)""")
        self.db.execute("""CREATE TABLE empty_test(
                id  INTEGER PRIMARY KEY,
                val TEXT,
                age INTEGER)""")

    def test_select_returns_list_of_row_as_dicts(self):
        self.assertEqual(self.db.execute("SELECT * FROM test"), [])
        for row in self.rows:
            self.db.execute("INSERT INTO test(val, age) VALUES(?, ?)",
                            row["val"], row["age"])
        self.assertEqual(self.db.execute(
            "SELECT * FROM test"), self.rows)

    def test_insert_returns_last_row_id(self):
        self.assertEqual(self.db.execute(
            "INSERT INTO test(val) VALUES('one')"), 1)
        self.assertEqual(self.db.execute(
            "INSERT INTO test(val) VALUES('two')"), 2)

    def test_replace_returns_last_row_id(self):
        """ REPLACE allows deletion and replacement of indexed values. """
        for row in self.rows:
            self.db.execute("INSERT INTO test(val, age) VALUES(?, ?)",
                            row["val"], row["age"])
        self.db.execute("CREATE UNIQUE INDEX idx_val ON test(val)")
        self.assertEqual(self.db.execute(
            "INSERT INTO test(val, age) VALUES(?, ?)", "man", 100), None)
        self.assertEqual(self.db.execute(
            "REPLACE INTO test (val, age) VALUES(?, ?)", "man", 100), 5)
        self.assertEqual(self.db.execute(
            "SELECT age FROM test WHERE val=?", "man"), [{"age": 100}])

    def test_update_returns_affected_row_count(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(val) VALUES(?)", row["val"])
        self.assertEqual(self.db.execute(
            "UPDATE test SET val=? WHERE id>?", 'puff', 1), 3)

    def test_delete_returns_affected_row_count(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(val, age) VALUES(?, ?)",
                            row["val"], row["age"])
        self.assertEqual(self.db.execute(
            "DELETE FROM test WHERE id>?", 1), 3)

    def test_delete_all(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(val, age) VALUES(?, ?)",
                            row["val"], row["age"])
        self.assertEqual(self.db.execute(
            "DELETE FROM test"), 4)

    def test_exists_table(self):
        """ Return True if the table exists, else False. """
        self.assertEqual(self.db.exists_table("test"), True)
        self.assertEqual(self.db.exists_table("not_test"), False)

    def test_column_names(self):
        columns = ["id", "val", "age"]
        not_columns = None
        for row in self.rows:
            self.db.execute("INSERT INTO test(val, age) VALUES(?, ?)",
                            row["val"], row["age"])
        self.assertEqual(self.db.column_names("test"), columns)
        self.assertEqual(self.db.column_names("not_test"), not_columns)
        self.assertEqual(self.db.column_names("empty_test"), columns)

    def test_column_totals(self):
        totals = {"id": 4, "val": 4, "age": 3}
        not_totals = None
        empty_totals = {"id": 0, "val": 0, "age": 0}
        for row in self.rows:
            self.db.execute("INSERT INTO test(val, age) VALUES(?, ?)",
                            row["val"], row["age"])
        self.assertEqual(self.db.column_totals("test"), totals)
        self.assertEqual(self.db.column_totals("not_test"), not_totals)
        self.assertEqual(self.db.column_totals("empty_test"), empty_totals)

    def test_row_total(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(val, age) VALUES(?, ?)",
                            row["val"], row["age"])
        self.assertEqual(self.db.row_total("test"), 4)
        self.assertEqual(self.db.row_total("not_test"), None)
        self.assertEqual(self.db.row_total("empty_test"), 0)

    def test_column_summary(self):
        summary = ("ID, Name, Type, NotNull, DefaultVal, PrimaryKey\n"
                   "0 id INTEGER 0 None 1\n"
                   "1 val TEXT 0 None 0\n"
                   "2 age INTEGER 0 None 0\n")
        captured_output = io.StringIO()
        sys.stdout = captured_output
        self.db.column_summary("test")
        sys.stdout = sys.__stdout__
        self.assertMultiLineEqual(captured_output.getvalue(), summary)

    def test_to_csv(self):
        lines = [
                  ["id", "val", "age"],
                  ["1", "dog", "10"],
                  ["2", "cat", "20"],
                  ["3", "man", "30"],
                  ["4", "rat", ""]
                ]

        for row in self.rows:
            self.db.execute("INSERT INTO test(val, age) VALUES(?, ?)",
                            row["val"], row["age"])
        self.db.to_csv("test")
        with open("test.csv") as csvfile:
            reader = csv.reader(csvfile)
            csv_lines = list(reader)
        self.assertListEqual(lines, csv_lines)

    def tearDown(self):
        self.db.execute("DROP TABLE test")
        self.db.execute("DROP TABLE empty_test")

    @classmethod
    def tearDownClass(cls):
        cls.db.execute("DROP TABLE IF EXISTS test")
        cls.db.execute("DROP TABLE IF EXISTS empty_test")
        os.remove("unit_test.db")
        os.remove("test.csv")


if __name__ == '__main__':
    unittest.main()
