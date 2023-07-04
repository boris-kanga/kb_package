import os
import sqlite3

import cx_Oracle


class BaseDBLite:
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self.db_object = None

    @staticmethod
    def execute(cursor, script, params):
        ...

    @staticmethod
    def connect(*args, **kwargs):
        ...

    def get_cursor(self):
        self.db_object = self.connect(*self._args, **self._kwargs)
        return self.db_object.cursor()

    @staticmethod
    def _fetchone(cursor, limit=None):
        if limit is None:
            limit = 1.8e308
        index_data = 0
        while index_data < limit:
            row = cursor.fetchone()
            if not row:
                break
            index_data += 1
            yield row

    def sql_select(self, script, params=None, limit=None, dict_res=False, retrieve=True):
        cursor = self.get_cursor()
        self.execute(cursor, script, params)
        if not retrieve:
            return

        columns = [col[0] for col in cursor.description or []]
        data = []
        for row in self._fetchone(cursor, limit=limit):
            data.append(dict(zip(columns, row)) if dict_res else row)
        return data


class OracleBDLite(BaseDBLite):
    @staticmethod
    def execute(cursor, script, params):
        cursor.execute(script, **(params or {}))

    @staticmethod
    def connect(host="localhost",
                user="root", password=None,
                port=1521,
                **kwargs) -> cx_Oracle.Connection:
        service_name = kwargs.get("service_name")
        try:
            if os.environ.get("ORACLE_INSTANT_CLIENT_PATH"):
                try:
                    cx_Oracle.init_oracle_client(lib_dir=os.environ.get("ORACLE_INSTANT_CLIENT_PATH"))
                    # this can raise: cx_Oracle.ProgrammingError if already initialized
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


class LiteSQLite(BaseDBLite):
    @staticmethod
    def execute(cursor, script, params):
        cursor.execute(script, *(params or []))

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
            ex.args = ["Une erreur lors que la connexion à la base de donnée --> " + str(ex.args[0])] + \
                      list(ex.args[1:])
            raise ex


if __name__ == '__main__':
    file = r"C:\Users\FBYZ6263\Documents\WORK_FOLDER\B2B\Mobile\b2b_mobile_local_db"
    sqlite_db = LiteSQLite(
        file_name=file)

    dataset = sqlite_db.sql_select("select * from evolution", dict_res=True, limit=5)
    from kb_package.database import SQLiteDB

    # print(SQLiteDB(file_name=file).run_script("select * from evolution", dict_res=True, limit=5))
    print(dataset[0])

