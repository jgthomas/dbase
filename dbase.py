import sqlite3
import re
import csv


class Database:
    """
    Wrapper and utility methods for sqlite.

    """
    def __init__(self, dbname):
        self.db = dbname
        try:
            self.conn = sqlite3.connect(self.db, detect_types=sqlite3.PARSE_DECLTYPES)
            self.conn.row_factory = sqlite3.Row
            self.cur = self.conn.cursor()
        except sqlite3.Error:
            print("Error connecting to the database\n")

    def close(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def execute(self, statement, *params):
        """ Execute SQL statement, returning appropriately. """
        try:
            with self.conn:
                self.cur.execute(statement, params)
                if re.search(r"^\s*SELECT", statement, re.IGNORECASE):
                    rows = self.cur.fetchall()
                    return [dict(row) for row in rows]
                elif re.search(r"^\s*INSERT", statement, re.IGNORECASE):
                    return self.cur.lastrowid
                elif re.search(r"^\s*(?:DELETE|UPDATE)", statement, re.IGNORECASE):
                    return self.cur.rowcount
                return True
        except sqlite3.IntegrityError:
            return None

    def exists_table(self, table):
        """ Check that table exists. """
        query = "SELECT 1 FROM sqlite_master WHERE type='table' and name = ?"
        return self.cur.execute(query, (table,)).fetchone() is not None

    def columns(self, table):
        """ Return column names from the specified table. """
        if self.exists_table(table):
            self.cur.execute("SELECT * FROM {0}".format(table))
            columns = self.cur.fetchone().keys()
            return columns
        return None

    def to_csv(self, table):
        """ Convert the specified table to csv format. """
        if self.exists_table(table):
            rows = self.execute("SELECT * FROM {0}".format(table))
            with open('output.csv', 'w') as csvfile:
                fieldnames = self.columns(table)
                dict_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                dict_writer.writeheader()
                dict_writer.writerows(rows)


class IterableTable:
   """ 
   Turns a database table into an iterable. 

   """

   def __init__(self, dbname, table):
      self._db = Database(dbname)
      self.table = table

   def exists_table(self):
       return self._db.exists_table(self.table)

   def columns(self):
       return self._db.columns(self.table)

   def __getitem__(self, item):
        if self.exists_table:
            statement = "SELECT * FROM {0} LIMIT ?, 1".format(self.table)
            self._db.cur.execute(statement, (item,))
            row = self._db.cur.fetchone()
            return dict(row)
