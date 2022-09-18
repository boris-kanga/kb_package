# -*- coding: utf-8 -*-
"""
The SQLite manager.
Use for run easily mysql requests
"""

import sqlite3
from kb_package.database.basedb import BaseDB
from kb_package.tools import INFINITE


class SQLiteDB(BaseDB):

    @staticmethod
    def connect(file_name="database.db", **kwargs) -> sqlite3.Connection:
        """
        Making the connexion to the mysql database
        Args:
            file_name: str, file name path
        Returns: the connexion object reach

        """
        try:
            return sqlite3.connect(file_name)
        except Exception as ex:
            raise Exception(
                "Une erreur lors que la connexion à la base de donnée: "
                + str(ex)
            )

    def last_insert_rowid_logic(self, cursor=None, table_name=None):
        if table_name is not None:
            table_name = " FROM " + str(table_name)
        else:
            table_name = ""
        return self._execute(cursor, "select last_insert_rowid()"+table_name)

    def _cursor(self):
        return self.db_object.cursor()

    @staticmethod
    def _execute(cursor, script, params=None, ignore_error=False, **kwargs):
        """
        use to make preparing requests
        Args:
            cursor: mysql.connector.cursor.MySQLCursor
            script: str, the prepared requests
            params: list|tuple|dict, params for the mysql prepared requests
            ignore_error: bool

        Returns: mysql.connector.cursor.MySQLCursor, the cu

        """
        args = [script]
        if params is None:
            pass
        elif isinstance(params, (tuple, list)):
            params = tuple(params)
            args.append(params)

        else:
            params = (params,)
            args.append(params)
        try:
            cursor.execute(*args)
            return cursor
        except Exception as ex:
            if ignore_error:
                return None
            raise Exception(ex)

    @staticmethod
    def get_all_data_from_cursor(cursor, limit=INFINITE, dict_res=False):
        if dict_res:
            columns = [col[0] for col in cursor.description]
            cursor.row_factory = lambda *args: dict(zip(columns, args[1]))
        data = []
        try:
            data = cursor.fetchall()
            if limit.__class__ == INFINITE.__class__:
                pass
            else:
                data = data[:limit]
        except (Exception, sqlite3.ProgrammingError):
            pass
        if limit == 1 and len(data):
            return data[0]
        return data

    @staticmethod
    def prepare_insert_data(data: list):
        return ["?" for _ in data]

    def dump(self, dump_file='dump.sql'):
        self.reload_connexion()
        with open(dump_file, 'w') as f:
            for line in self.db_object.iterdump():
                f.write('%s\n' % line)


if __name__ == '__main__':
    db_object = SQLiteDB(file_name=None)

    db_object.create_table(r"C:\Users\FBYZ6263\Downloads\LISTE PUSH OFFRE CABINE 310822.xlsx",
                           auto_increment_field=True)
