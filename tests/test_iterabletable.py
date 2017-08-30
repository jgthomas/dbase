import unittest
import os
from dbase import Database
from iter_table import IterableTable


class IterableTableTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = Database("unit_test.db")
        cls.db.execute("""CREATE TABLE test(
                id  INTEGER PRIMARY KEY, 
                name TEXT,
                age INTEGER)""")
        cls.rows = [
                {"id": 1, "name": "dog", "age": 10},
                {"id": 2, "name": "cat", "age": 20},
                {"id": 3, "name": "man", "age": 30},
                {"id": 4, "name": "rat", "age": None}
        ]
        for row in cls.rows:
            cls.db.execute("INSERT INTO test(name, age) VALUES(?, ?)",
                            row["name"], row["age"])
        cls.table = IterableTable("unit_test.db", "test")

    def test_take_items_from_table(self):
        taken = [
                  {"id": 1, "name": "dog", "age": 10},
                  {"id": 2, "name": "cat", "age": 20}
                ]
        self.assertEqual(self.table.take(2), taken)

    def test_take_beyond_range(self):
        with self.assertRaises(IndexError) as r:
            self.table.take(10)
        self.assertIn("IterableTable index out of range", str(r.exception))

    def test_head_returns_one(self):
        taken = [{"id": 1, "name": "dog", "age": 10}]
        self.assertEqual(self.table.head(), taken)

    @classmethod
    def tearDownClass(cls):
        cls.db.execute("DROP TABLE IF EXISTS test")
        os.remove("unit_test.db")


if __name__ == '__main__':
    unittest.main()
