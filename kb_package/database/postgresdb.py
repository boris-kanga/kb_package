# -*- coding: utf-8 -*-
"""
The Mysql manager.
Use for run easily mysql requests

Required psycopg2~=2.9.3
"""

import psycopg2
from kb_package.tools import INFINITE
from kb_package.database.basedb import BaseDB


class PostgresDB(BaseDB):
    @property
    def _get_name(self):
        return self.__class__.__name__

    DEFAULT_PORT = 5432

    @staticmethod
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
    def get_all_data_from_cursor(cursor, limit=INFINITE, dict_res=False):

        data = []
        try:
            row = cursor.fetchone()

            while row is not None and len(data) < limit:
                data.append(row)
                row = cursor.fetchone()
        except (Exception, psycopg2.DatabaseError):
            pass
        if limit == 1:
            if len(data):
                return data[0]
            return None
        return data

    @staticmethod
    def _execute(cursor, script, params=None, ignore_error=False,
                 connexion=None):
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
            raise Exception(ex)

    @staticmethod
    def get_add_increment_field_code(field_name="id"):
        return str(field_name or "id") + " SERIAL PRIMARY KEY"
