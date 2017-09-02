import sqlite3
import re
import csv


class Database:
    """
    Wrapper and utility methods for sqlite.

    db     :  Name of the database file.
    trace  :  An optional callback function to log or
              print each SQL statement as executed.

    """

    def __init__(self, db, trace=None):
        self.db = db
        self.trace = trace
        try:
            self.conn = sqlite3.connect(self.db,
                                        detect_types=sqlite3.PARSE_DECLTYPES)
            self.conn.row_factory = sqlite3.Row
            self.conn.set_trace_callback(self.trace)
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

    def __repr__(self):
        return ("{0}('{1}', trace={2})"
                .format(self.__class__.__name__, self.db, self.trace))

    def __str__(self):
        return ("{0} connection\n"
                "Database filename: {1}\n"
                "Traceback function: {2}\n"
                .format(self.__class__.__name__,
                        self.db,
                        self.trace))

    def execute(self, statement, *params):
        """ Execute SQL statement, returning appropriately. """
        try:
            with self.conn:
                self.cur.execute(statement, params)
                if re.search(r"^\s*(?:SELECT|PRAGMA)", statement, re.I):
                    rows = self.cur.fetchall()
                    return [dict(row) for row in rows]
                elif re.search(r"^\s*(?:INSERT|REPLACE)", statement, re.I):
                    return self.cur.lastrowid
                elif re.search(r"^\s*(?:DELETE|UPDATE)", statement, re.I):
                    return self.cur.rowcount
                return True
        except sqlite3.IntegrityError:
            return None

    def exists_table(self, table):
        """ Check that table exists, guard against SQL injection. """
        query = """SELECT 1
                     FROM sqlite_master
                    WHERE type = 'table'
                      AND name = ?"""
        return self.cur.execute(query, (table,)).fetchone() is not None

    def column_data(self, table):
        """ Return data on columns. """
        if self.exists_table(table):
            self.cur.execute("PRAGMA TABLE_INFO({0})".format(table))
            return self.cur.fetchall()

    def column_names(self, table):
        """ Return column names. """
        column_data = self.column_data(table)
        if column_data:
            return [name for _, name, *_ in column_data]

    def col_names(self, columns):
        return [column["name"] for column in columns]

    def columns(self, table):
        if self.exists_table(table):
            column_data = self.execute("PRAGMA TABLE_INFO({0})".format(table))
            column_names = self.col_names(column_data)
            return dict(zip(column_names, column_data))

    def rows(self, table):
        """ Return number of rows in the table. """
        if self.exists_table(table):
            result, *_ = self.execute(
                "SELECT COUNT(*) AS row_total FROM {}".format(table))
            return result["row_total"]

    def column_summary(self, table):
        """ Print summary of column details. """
        column_data = self.column_data(table)
        if column_data:
            print("ID, Name, Type, NotNull, DefaultVal, PrimaryKey")
            for column in column_data:
                print(*column)

    def column_totals(self, table):
        """ Return number of non-null values in each column. """
        column_total_values = {}
        column_names = self.column_names(table)
        if column_names:
            for column_name in column_names:
                data, *_ = self.execute(
                    """SELECT COUNT({0}) AS column_total
                         FROM {1}
                        WHERE {0}
                           IS NOT NULL""".format(column_name, table))
                column_total_values[column_name] = data["column_total"]
            return column_total_values

    def row_total(self, table):
        """ Return number of rows in the table. """
        if self.exists_table(table):
            result, *_ = self.execute(
                "SELECT COUNT(*) AS row_total FROM {}".format(table))
            return result["row_total"]

    def to_csv(self, table, outfile=None):
        """ Convert table to csv file. """
        if self.exists_table(table):
            rows = self.execute("SELECT * FROM {0}".format(table))
            fieldnames = self.column_names(table)
            if not outfile:
                outfile = ''.join([table, ".csv"])
            with open(outfile, 'w') as csvfile:
                dict_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                dict_writer.writeheader()
                dict_writer.writerows(rows)

    def shell(self):
        welcome = "\nSimple sqlite shell"
        commands = ("\nBuild an SQL statement, or:\n\n"
                    "'quit'  : exit shell\n"
                    "'help'  : show this message\n"
                    "'clear' : reset statement buffer\n"
                    "'state' : show statement buffer\n")
        prompt = "{0}>".format(self.db)
        confirmation = "Is this OK (y/n): "
        statement = ""
        print(welcome)
        print(commands)
        while True:
            line = input(prompt)
            if line.lower() == "quit":
                break
            if line.lower() == "help":
                print(commands)
                continue
            if line.lower() == "clear":
                statement = ""
                continue
            if line.lower() == "state":
                print(statement)
                continue
            statement = " ".join([statement, line])
            if sqlite3.complete_statement(statement):
                try:
                    statement = statement.strip()
                    if re.search((r"^\s*(?:DELETE|UPDATE|INSERT"
                                  r"|REPLACE|DROP|CREATE|ALTER)"),
                                 statement, re.I):
                        print("Executing: '{0}' will permanently "
                              "alter the database!\n"
                              .format(statement))
                        confirm = input(confirmation).lower()
                        if confirm not in ["y", "yes"]:
                            statement = ""
                            continue
                        self.cur.execute(statement)
                    elif statement.upper().startswith("SELECT"):
                        print(self.execute(statement))
                    else:
                        print(commands)
                except sqlite3.Error as e:
                    print("An error occurred:", e.args[0])
                statement = ""
