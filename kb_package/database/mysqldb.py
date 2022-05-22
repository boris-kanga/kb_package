# -*- coding: utf-8 -*-
"""
The Mysql manager.
Use for run easily mysql requests
"""


import mysql.connector
from kb_package.tools import INFINITE
from .basedb import BaseDB


class MysqlDB(BaseDB):

    @staticmethod
    def connect(
            host="127.0.0.1", user="root", password="", db_name=None,
            port=BaseDB.DEFAULT_PORT, filename=None
    ) -> mysql.connector.MySQLConnection:
        """
        Making the connexion to the mysql database
        Args:
            host: str, the Mysql server ip
            user: str, the username for mysql connexion
            password: str, the password
            db_name: str, the database name
            port:
            filename:

        Returns:mysql.connector.MySQLConnection, the connexion object reach

        """
        try:
            return mysql.connector.connect(
                host=host, user=user, passwd=password, database=db_name,
                port=port
            )
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
        try:
            cursor.execute(script, params=params)
            return cursor
        except Exception as ex:
            if ignore_error:
                return None
            raise Exception(ex)

    @staticmethod
    def get_all_data_from_cursor(cursor, limit=INFINITE):

        data = []
        try:
            data = cursor.fetchall()[:limit]
        except (Exception, mysql.connector.errors.ProgrammingError):
            pass
        if limit == 1 and len(data):
            return data[0]
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
                except (mysql.connector.errors.ProgrammingError, Exception):
                    datas = None
            return datas
