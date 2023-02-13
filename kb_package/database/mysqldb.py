# -*- coding: utf-8 -*-
"""
The Mysql manager.
Use for run easily mysql requests



required mysql-connector-python~=8.0.25
"""
import traceback

import mysql.connector
from kb_package.tools import INFINITE
from kb_package.database.basedb import BaseDB


class MysqlDB(BaseDB):
    DEFAULT_PORT = 3306

    @property
    def _get_name(self):
        return self.__class__.__name__

    def _is_connected(self):
        try:
            return self.db_object.is_connected()
        except (AttributeError, mysql.connector.errors.DatabaseError, Exception):
            return False

    @staticmethod
    def connect(
            host="127.0.0.1", user="root", password="", db_name=None,
            port=DEFAULT_PORT, **kwargs
    ) -> mysql.connector.MySQLConnection:
        """
        Making the connexion to the mysql database
        Args:
            host: str, the Mysql server ip
            user: str, the username for mysql connexion
            password: str, the password
            db_name: str, the database name
            port:

        Returns:mysql.connector.MySQLConnection, the connexion object reach

        """
        try:
            return mysql.connector.connect(
                host=host, user=user, passwd=password, database=db_name,
                port=port or MysqlDB.DEFAULT_PORT
            )
        except Exception as ex:
            ex.args = ["Une erreur lors que la connexion à la base de donnée --> " + str(ex.args[0])] + \
                      list(ex.args[1:])
            raise ex

    @staticmethod
    def prepare_insert_data(data: dict):
        return ["?" for _ in data], list(data.values())

    def _cursor(self):
        return self.db_object.cursor(dictionary=True)

    @staticmethod
    def _execute(cursor, script, params=None, ignore_error=False, method="single", **kwargs):
        """
        use to make preparing requests
        Args:
            cursor: mysql.connector.cursor.MySQLCursor
            script: str, the prepared requests
            params: list|tuple|dict, params for the mysql prepared requests
            ignore_error: bool

        Returns: mysql.connector.cursor.MySQLCursor, the cu

        """
        if method == "many":
            method = "executemany"
        else:
            method = "execute"
        try:
            getattr(cursor, method)(script, params=params)
            return cursor
        except Exception as ex:
            traceback.print_exc()
            if ignore_error:
                return None
            raise Exception(ex)

    @staticmethod
    def get_add_increment_field_code(field_name="id"):
        return str(field_name or "id") + " MEDIUMINT PRIMARY KEY AUTOINCREMENT"

    @staticmethod
    def get_all_data_from_cursor(cursor, limit=INFINITE, dict_res=False):
        if not cursor.with_rows:
            return None
        MysqlDB.LAST_REQUEST_COLUMNS = []

        data = []
        try:
            got = False
            while len(data) < limit:
                row = cursor.fetchone()
                if not row:
                    break
                if not got:
                    MysqlDB.LAST_REQUEST_COLUMNS = list(row.keys())
                    got = True
                if not dict_res:
                    row = list(row.values())
                data.append(row)
        except (Exception, mysql.connector.errors.ProgrammingError):
            pass
        if limit == 1:
            if len(data):
                return data[0]
            return None
        return data

    def run_script_file(self, path, params=None):
        """
        Run a specific sql file
        Args:
            path: str, the file path
            params: list|tuple|dict, params for the mysql prepared requests

        Returns: data, mysql results

        """
        with open(path) as f:
            script = f.read()
            with self.get_cursor() as cursor:
                try:
                    datas = MysqlDB._execute(cursor, script, params).fetchall()
                except (mysql.connector.errors.ProgrammingError,
                        Exception) as ex:
                    traceback.print_exc()
                    self._print_error(ex)
                    datas = None
            return datas
