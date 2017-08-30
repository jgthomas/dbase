

from dbase import Database


class IterableTable:
    """
    Turns a database table into an iterable.

    """

    def __init__(self, dbname, table):
        self.dbname = dbname
        self._db = Database(self.dbname)
        self.table = table

    def __repr__(self):
        return "IterableTable({0}, {1})".format(self.dbname, self.table)

    def exists_table(self):
        return self._db.exists_table(self.table)

    def __getitem__(self, item):
        if self.exists_table:
            statement = "SELECT * FROM {0} LIMIT ?, 1".format(self.table)
            self._db.cur.execute(statement, (item,))
            row = self._db.cur.fetchone()
            if row is not None:
                return dict(row)
            raise StopIteration

    def take(self, n):
        try:
            return [self.__getitem__(i) for i in range(0, n)]
        except StopIteration as exc:
            raise IndexError("IterableTable index out of range") from exc

    def head(self):
        return self.take(1)
