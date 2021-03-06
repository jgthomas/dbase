import sqlite3
import re
import csv
import json


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

    def _exists_table(self, table):
        """ Check that table exists, guard against SQL injection. """
        query = """SELECT 1
                     FROM sqlite_master
                    WHERE type = 'table'
                      AND name = ?"""
        return self.cur.execute(query, (table,)).fetchone() is not None

    def _column_names(self, columns):
        """ Return column names. """
        return [column["name"] for column in columns]

    def column_config(self, table):
        """ Return data about columns. """
        if self._exists_table(table):
            column_data = self.execute("PRAGMA TABLE_INFO({0})".format(table))
            column_names = self._column_names(column_data)
            return dict(zip(column_names, column_data))

    def row_count(self, table):
        """ Return number of rows in the table. """
        if self._exists_table(table):
            result, *_ = self.execute(
                "SELECT COUNT(*) AS row_total FROM {}".format(table))
            return result["row_total"]

    def column_totals(self, table):
        """ Return number of non-null values in each column. """
        column_total_values = {}
        try:
            column_names = self.column_config(table).keys()
        except AttributeError:
            column_names = None
        if column_names:
            for column_name in column_names:
                data, *_ = self.execute(
                    """SELECT COUNT({0}) AS column_total
                         FROM {1}
                        WHERE {0}
                           IS NOT NULL""".format(column_name, table))
                column_total_values[column_name] = data["column_total"]
            return column_total_values

    @staticmethod
    def query_to_file(rows, filetype, outfile=None):
        """
        Return a query as a file.

        rows     : list of sqlite3.Row objects
        filetype : filetype to output query result
        outfile  : name of the output file

        """
        fieldnames = rows[0].keys()
        rows = [dict(row) for row in rows]

        if not outfile:
            outfile = ''.join(["query.", filetype])

        def to_csv():
            with open(outfile, 'w') as csvfile:
                dict_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                dict_writer.writeheader()
                dict_writer.writerows(rows)

        def to_json():
            with open(outfile, 'w') as jsonfile:
                json.dump(rows, jsonfile)

        filetypes = {"csv": to_csv,
                     "json": to_json}

        try:
            filetypes[filetype]()
        except KeyError as e:
            print("'{0}' is not a recognised filetype. "
                  "Supported filetypes: {1}"
                  .format(filetype, ', '.join(list(filetypes.keys()))))
            raise e

    def execute(self, statement, *params, filetype=None):
        """
        Execute SQL statement, returning appropriately.

        statement : valid SQL query string
        params    : parameters to fill placeholder values in statement
        filetype  : return the query as a file of this type

        """
        try:
            with self.conn:
                self.cur.execute(statement, params)
                if re.search(r"^\s*(?:SELECT|PRAGMA)", statement, re.I):
                    rows = self.cur.fetchall()
                    if filetype:
                        self.query_to_file(rows, filetype)
                    else:
                        return [dict(row) for row in rows]
                elif re.search(r"^\s*(?:INSERT|REPLACE)", statement, re.I):
                    return self.cur.lastrowid
                elif re.search(r"^\s*(?:DELETE|UPDATE)", statement, re.I):
                    return self.cur.rowcount
                return True
        except sqlite3.IntegrityError:
            return None

    def as_json(self, statement, *params):
        """ Return query as a json string. """
        return json.dumps(self.execute(statement, *params))

    def shell(self):
        """ Simple sqlite shell for running interactively. """
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
