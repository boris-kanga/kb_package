# -*- coding: utf-8 -*-
"""
The SQLite manager.
Use for run easily mysql requests
"""

import sqlite3
from .basedb import BaseDB
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
        if isinstance(params, (tuple, list)):
            params = tuple(params)
        else:
            params = (params,)
        try:
            cursor.execute(script, params)
            return cursor
        except Exception as ex:
            if ignore_error:
                return None
            raise Exception(ex)

    @staticmethod
    def get_all_data_from_cursor(cursor, limit=INFINITE):

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
