import unittest
from dbase import Database


class DatabaseTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = Database("unit_test.db")

    @classmethod
    def tearDownClass(cls):
        cls.db.execute("DROP TABLE IF EXISTS test")

    def setUp(self):
        self.db.execute("""CREATE TABLE test(
                id  INTEGER PRIMARY KEY, 
                val TEXT)""")

    def tearDown(self):
        self.db.execute("DROP TABLE test")

    def test_insert_returns_last_row_id(self):
        self.assertEqual(self.db.execute(
            "INSERT INTO test(val) VALUES('one')"), 1)
        self.assertEqual(self.db.execute(
            "INSERT INTO test(val) VALUES('two')"), 2)


if __name__ == '__main__':
    unittest.main()
