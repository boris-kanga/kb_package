# -*- coding: utf-8 -*-
"""
The Mysql manager.
Use for run easily mysql requests



required cx-Oracle
"""
import os

import cx_Oracle

from kb_package.database.basedb import BaseDB, for_csv
from kb_package.tools import INFINITE


class OracleDB(BaseDB):
    DEFAULT_PORT = 1521

    @property
    def _get_name(self):
        return self.__class__.__name__

    def _is_connected(self):
        try:
            return self.db_object.ping() is None
        except (AttributeError, cx_Oracle.DatabaseError, Exception):
            return False

    @staticmethod
    def connect(host="localhost",
                user="root", password=None,
                port=DEFAULT_PORT,
                **kwargs):
        service_name = kwargs.get("service_name")
        try:
            if os.environ.get("ORACLE_INSTANT_CLIENT_PATH"):
                try:
                    cx_Oracle.init_oracle_client(lib_dir=os.environ.get("ORACLE_INSTANT_CLIENT_PATH"))
                except (Exception, TypeError):
                    pass
            dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
            return cx_Oracle.connect(user=user,
                                     password=password,
                                     dsn=dsn)
        except Exception as ex:
            ex.args = ["Une erreur lors que la connexion à la base de donnée --> " + str(ex.args[0])] + \
                      list(ex.args[1:])
            raise ex

    def _cursor(self):
        return self.db_object.cursor()

    @staticmethod
    def prepare_insert_data(data: dict):
        return [":" + str(d) for d in data.keys()], data

    @staticmethod
    def get_all_data_from_cursor(cursor, limit=INFINITE, dict_res=False, export_name=None, sep=";"):
        columns = [col[0] for col in cursor.description or []]
        OracleDB.LAST_REQUEST_COLUMNS = columns
        if dict_res and export_name is None:
            cursor.rowfactory = lambda *args: dict(zip(columns, args))

        data = []
        try:
            export_file = type("MyTempFile", (), {"__enter__": lambda *args: 1, "__exit__": lambda *args: 1})()
            if export_name is not None:
                export_file = open(export_name, "w")
            with export_file:
                if export_name is not None:
                    export_file.write(for_csv(columns, sep=sep) + "\n")
                index_data = 0
                while index_data < limit:
                    row = cursor.fetchone()
                    if not row:
                        break
                    if export_name is not None:
                        export_file.write(for_csv(row, sep=sep) + "\n")
                    else:
                        data.append(row)
                    index_data += 1
            if export_name is not None:
                return
        except (Exception, cx_Oracle.Error):
            pass
        if limit == 1:
            if len(data):
                return data[0]
            return None
        return data

    @staticmethod
    def _execute(cursor, script, params=None, ignore_error=False, method="single", **kwargs):
        OracleDB.LAST_SQL_CODE_RUN = script
        if method == "many":
            method = "executemany"
        else:
            method = "execute"
        try:
            if isinstance(params, (list, tuple)):
                getattr(cursor, method)(script, params)
                return cursor
            elif not isinstance(params, dict):
                params = {}
            getattr(cursor, method)(script, **params)
            return cursor
        except Exception as ex:
            if ignore_error:
                return None

            raise Exception(ex)

    def _get_table_schema(self, table_name):
        """
        cursor.execute("select * from " + table_name + " where location_id = 1000")
        columns = [col[0] for col in cursor.description]

        """


if __name__ == '__main__':
    pass



