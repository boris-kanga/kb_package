# -*- coding: utf-8 -*-
"""
The Mysql manager.
Use for run easily mysql requests

Required psycopg2~=2.9.3
"""

import psycopg2
from kb_package.tools import Cdict, many_try
from kb_package.database.basedb import BaseDB


class PostgresDB(BaseDB):
    DEFAULT_PORT = 5432

    @property
    def _get_name(self):
        return self.__class__.__name__

    def _is_connected(self):
        try:
            return not self.db_object.closed
        except (AttributeError, psycopg2.Error, Exception):
            return False

    @staticmethod
    @many_try(max_try=1, sleep_time=0, error_manager_key="BD_POSTGRES")
    def connect(
        host="127.0.0.1", user="root", password="", db_name=None,
            port=DEFAULT_PORT, **kwargs):
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
            return psycopg2.connect(host=host, user=user,
                                    dbname=db_name,
                                    password=password, port=port)
        except Exception as ex:
            ex.args = ["Une erreur lors que la connexion à la base de donnée --> " + str(ex.args[0])] + \
                      list(ex.args[1:])
            raise ex

    def _cursor(self):
        return self.db_object.cursor()

    @staticmethod
    def _get_cursor_description(cursor):
        return Cdict(columns=[desc[0] for desc in cursor.description or []])

    @staticmethod
    @many_try(max_try=1, sleep_time=0, error_manager_key="BD_POSTGRES")
    def _execute(cursor, script, params=None, ignore_error=False, connexion=None, **kwargs):
        """
        use to make preparing requests
        Args:
            cursor:
            script: str, the prepared requests
            params: list|tuple|dict, params for the mysql prepared requests
            connexion:

        Returns: the cursor after make request

        """
        if isinstance(params, (tuple, list)):
            params = tuple(params)
        else:
            params = (params,)
        try:
            cursor.execute(script, params)
            return cursor
        except Exception as ex:
            connexion.rollback()
            if ignore_error:
                return None
            raise ex

    @staticmethod
    def get_add_increment_field_code(field_name="id"):
        return str(field_name or "id") + " SERIAL PRIMARY KEY"
