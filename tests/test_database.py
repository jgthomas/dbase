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
                {"id": 1, "name": "dog", "age": 10},
                {"id": 2, "name": "cat", "age": 20},
                {"id": 3, "name": "man", "age": 30},
                {"id": 4, "name": "rat", "age": None}
        ]
        cls.maxDiff = None

    def setUp(self):
        self.db.execute("""CREATE TABLE test(
                id   INTEGER PRIMARY KEY,
                name TEXT,
                age  INTEGER)""")
        self.db.execute("""CREATE TABLE empty_test(
                id   INTEGER PRIMARY KEY,
                name TEXT,
                age  INTEGER)""")

    def test_select_returns_list_of_row_as_dicts(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        self.assertEqual(self.db.execute(
            "SELECT * FROM test ORDER BY id"), self.rows)
        self.assertEqual(self.db.execute(
            "SELECT * FROM empty_test ORDER BY id"), [])

    def test_insert_returns_last_row_id(self):
        self.assertEqual(self.db.execute(
            "INSERT INTO test(name, age) VALUES(?, ?)", "wolf", 23), 1)
        self.assertEqual(self.db.execute(
            "INSERT INTO test(name, age) VALUES(?, ?)", "tiger", 12), 2)

    def test_replace_returns_last_row_id(self):
        """ REPLACE allows deletion and insertion of changed indexed values. """
        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        self.db.execute("CREATE UNIQUE INDEX idx_name ON test(name)")
        self.assertEqual(self.db.execute(
            "INSERT INTO test(name, age) VALUES(?, ?)", "man", 100), None)
        self.assertEqual(self.db.execute(
            "REPLACE INTO test (name, age) VALUES(?, ?)", "man", 100), 5)
        self.assertEqual(self.db.execute(
            "SELECT age FROM test WHERE name=?", "man"), [{"age": 100}])

    def test_update_returns_affected_row_count(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        self.assertEqual(self.db.execute(
            "UPDATE test SET name=? WHERE id>?", 'puff', 1), 3)

    def test_delete_returns_affected_row_count(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        self.assertEqual(self.db.execute(
            "DELETE FROM test WHERE id>?", 1), 3)

    def test_delete_all(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        self.assertEqual(self.db.execute("DELETE FROM test"), 4)

    def test_exists_table(self):
        self.assertEqual(self.db.exists_table("test"), True)
        self.assertEqual(self.db.exists_table("empty_test"), True)
        self.assertEqual(self.db.exists_table("not_test"), False)

    def test_column_names(self):
        columns = ["id", "name", "age"]
        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        self.assertEqual(self.db.column_names("test"), columns)
        self.assertEqual(self.db.column_names("not_test"), None)
        self.assertEqual(self.db.column_names("empty_test"), columns)

    def test_column_summary_method(self):
        summary = {"id": {"cid": 0,
                          "name":"id",
                          "notnull": 0,
                          "type":"INTEGER",
                          "dflt_value": None,
                          "pk": 1},
                   "name": {"cid": 1,
                            "name": "name",
                            "notnull": 0,
                            "type":"TEXT",
                            "dflt_value": None,
                            "pk": 0},
                   "age": {"cid": 2,
                           "name": "age",
                           "notnull": 0, "type":
                           "INTEGER",
                           "dflt_value": None,
                           "pk": 0}
                   }
        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        self.assertDictEqual(self.db.columns("test"), summary)
        self.assertDictEqual(self.db.columns("empty_test"), summary)
        self.assertEqual(self.db.columns("not_test"), None)

    def test_row_summary(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        self.assertEqual(self.db.row_total("test"), 4)
        self.assertEqual(self.db.row_total("not_test"), None)
        self.assertEqual(self.db.row_total("empty_test"), 0)

    def test_column_totals(self):
        totals = {"id": 4, "name": 4, "age": 3}
        not_totals = None
        empty_totals = {"id": 0, "name": 0, "age": 0}
        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        self.assertEqual(self.db.column_totals("test"), totals)
        self.assertEqual(self.db.column_totals("not_test"), not_totals)
        self.assertEqual(self.db.column_totals("empty_test"), empty_totals)

    def test_row_total(self):
        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        self.assertEqual(self.db.row_total("test"), 4)
        self.assertEqual(self.db.row_total("not_test"), None)
        self.assertEqual(self.db.row_total("empty_test"), 0)

    def test_column_summary(self):
        summary = ("ID, Name, Type, NotNull, DefaultVal, PrimaryKey\n"
                   "0 id INTEGER 0 None 1\n"
                   "1 name TEXT 0 None 0\n"
                   "2 age INTEGER 0 None 0\n")
        captured_output = io.StringIO()
        sys.stdout = captured_output
        self.db.column_summary("test")
        sys.stdout = sys.__stdout__
        self.assertMultiLineEqual(captured_output.getvalue(), summary)

    def test_to_csv(self):
        lines = [
                  ["id", "name", "age"],
                  ["1", "dog", "10"],
                  ["2", "cat", "20"],
                  ["3", "man", "30"],
                  ["4", "rat", ""]
        ]

        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        self.db.to_csv("test")
        with open("test.csv") as csvfile:
            reader = csv.reader(csvfile)
            csv_lines = list(reader)
        self.assertListEqual(lines[0], csv_lines[0])
        self.assertListEqual(sorted(lines), sorted(csv_lines))

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
