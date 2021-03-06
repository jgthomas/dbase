import unittest
import os
import csv
import json
import operator
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
        for row in self.rows:
            self.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])

    def test_select_returns_list_of_row_as_dicts(self):
        self.assertEqual(self.db.execute(
            "SELECT * FROM test ORDER BY id"), self.rows)
        self.assertEqual(self.db.execute(
            "SELECT * FROM empty_test ORDER BY id"), [])

    def test_select_query_to_csv_file(self):
        lines = [
              ["id", "name", "age"],
              ["3", "man", "30"],
              ["4", "rat", ""]
        ]

        self.db.execute("SELECT * FROM test WHERE id>?", 2, filetype="csv")
        with open("query.csv") as csvfile:
            reader = csv.reader(csvfile)
            csv_lines = list(reader)
        self.assertListEqual(lines[0], csv_lines[0])
        self.assertListEqual(sorted(lines), sorted(csv_lines))

    def test_insert_returns_last_row_id(self):
        self.assertEqual(self.db.execute(
            "INSERT INTO test(name, age) VALUES(?, ?)", "wolf", 23), 5)
        self.assertEqual(self.db.execute(
            "INSERT INTO test(name, age) VALUES(?, ?)", "tiger", 12), 6)

    def test_replace_returns_last_row_id(self):
        """ REPLACE allows deletion and insertion of changed indexed values. """
        self.db.execute("CREATE UNIQUE INDEX idx_name ON test(name)")
        self.assertEqual(self.db.execute(
            "INSERT INTO test(name, age) VALUES(?, ?)", "man", 100), None)
        self.assertEqual(self.db.execute(
            "REPLACE INTO test (name, age) VALUES(?, ?)", "man", 100), 5)
        self.assertEqual(self.db.execute(
            "SELECT age FROM test WHERE name=?", "man"), [{"age": 100}])

    def test_update_returns_affected_row_count(self):
        self.assertEqual(self.db.execute(
            "UPDATE test SET name=? WHERE id>?", 'puff', 1), 3)

    def test_delete_returns_affected_row_count(self):
        self.assertEqual(self.db.execute(
            "DELETE FROM test WHERE id>?", 1), 3)

    def test_delete_all(self):
        self.assertEqual(self.db.execute("DELETE FROM test"), 4)
        self.assertEqual(self.db.execute("DELETE FROM empty_test"), 0)

    def test_exists_table(self):
        self.assertEqual(self.db._exists_table("test"), True)
        self.assertEqual(self.db._exists_table("empty_test"), True)
        self.assertEqual(self.db._exists_table("not_test"), False)

    def test_column_names(self):
        column_names = ["id", "name", "age"]
        columns = self.db.execute("PRAGMA TABLE_INFO(test)")
        self.assertListEqual(self.db._column_names(columns), column_names)
        columns = self.db.execute("PRAGMA TABLE_INFO(empty_test)")
        self.assertListEqual(self.db._column_names(columns), column_names)
        columns = self.db.execute("PRAGMA TABLE_INFO(not_test)")
        self.assertListEqual(self.db._column_names(columns), [])

    def test_column_config(self):
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
        self.assertDictEqual(self.db.column_config("test"), summary)
        self.assertDictEqual(self.db.column_config("empty_test"), summary)
        self.assertEqual(self.db.column_config("not_test"), None)

    def test_row_count(self):
        self.assertEqual(self.db.row_count("test"), 4)
        self.assertEqual(self.db.row_count("not_test"), None)
        self.assertEqual(self.db.row_count("empty_test"), 0)

    def test_column_totals(self):
        totals = {"id": 4, "name": 4, "age": 3}
        not_totals = None
        empty_totals = {"id": 0, "name": 0, "age": 0}
        self.assertDictEqual(self.db.column_totals("test"), totals)
        self.assertEqual(self.db.column_totals("not_test"), not_totals)
        self.assertDictEqual(self.db.column_totals("empty_test"), empty_totals)

    def test_query_to_file_csv(self):
        lines = [
              ["id", "name", "age"],
              ["1", "dog", "10"],
              ["2", "cat", "20"],
              ["3", "man", "30"],
              ["4", "rat", ""]
        ]

        self.db.cur.execute("SELECT * FROM test ORDER BY id")
        rows = self.db.cur.fetchall()
        self.db.query_to_file(rows, "csv")
        with open("query.csv") as csvfile:
            reader = csv.reader(csvfile)
            csv_lines = list(reader)
        self.assertListEqual(lines[0], csv_lines[0])
        self.assertListEqual(sorted(lines), sorted(csv_lines))

    def test_query_to_file_json(self):
        lines = [
                  {"id": 1, "name": "dog", "age": 10},
                  {"id": 2, "name": "cat", "age": 20},
                  {"id": 3, "name": "man", "age": 30},
                  {"id": 4, "name": "rat", "age": None}
        ]

        self.db.cur.execute("SELECT * FROM test ORDER BY id")
        rows = self.db.cur.fetchall()
        self.db.query_to_file(rows, "json")
        with open("query.json") as jsonfile:
            json_data = json.load(jsonfile)
        sorted_lines = sorted(lines, key=operator.itemgetter("id"))
        sorted_json = sorted(json_data, key=operator.itemgetter("id"))
        self.assertListEqual(sorted_json, sorted_lines)

    def test_query_to_file_unknown_type(self):
        self.db.cur.execute("SELECT * FROM test ORDER BY id")
        rows = self.db.cur.fetchall()
        with self.assertRaises(KeyError):
            self.db.query_to_file(rows, "unsupported")

    def test_as_json(self):
        json_ref = '[{"id": 1, "name": "dog", "age": 10}]'
        json_object = self.db.as_json("SELECT * FROM test WHERE id=?", 1)
        self.assertEqual(json_ref, json_object)

    def tearDown(self):
        self.db.execute("DROP TABLE test")
        self.db.execute("DROP TABLE empty_test")

    @classmethod
    def tearDownClass(cls):
        cls.db.execute("DROP TABLE IF EXISTS test")
        cls.db.execute("DROP TABLE IF EXISTS empty_test")
        os.remove("unit_test.db")
        os.remove("query.csv")
        os.remove("query.json")


if __name__ == '__main__':
    unittest.main()
