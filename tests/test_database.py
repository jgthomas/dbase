import unittest
from dbase import Database


class DatabaseTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.db = Database("unit_test.db")

    def setUp(self):
        self.db.execute("""CREATE TABLE test(
                id  INTEGER PRIMARY KEY, 
                val TEXT)""")

    @classmethod
    def tearDownClass(self):
        self.db.execute("DROP TABLE IF EXISTS test")

    def test_insert_returns_last_row_id(self):
        self.assertEqual(self.db.execute(
            "INSERT INTO test(val) VALUES('one')"), 1)
        self.assertEqual(self.db.execute(
            "INSERT INTO test(val) VALUES('two')"), 2)


if __name__ == '__main__':
    unittest.main()
