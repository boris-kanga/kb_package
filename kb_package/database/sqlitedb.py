# -*- coding: utf-8 -*-
"""
The SQLite manager.
Use for run easily mysql requests
"""

import sqlite3

from kb_package.database.basedb import BaseDB
from kb_package.tools import Cdict, many_try


class SQLiteDB(BaseDB):

    @property
    def _get_name(self):
        return self.__class__.__name__

    @staticmethod
    @many_try(max_try=100, error_got="database is locked", sleep_time=1,
              error_manager_key="BD_SQLITE")
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

    def last_insert_rowid_logic(self, cursor=None, table_name=None):
        if table_name is not None:
            table_name = " FROM " + str(table_name)
        else:
            table_name = ""
        return self._execute(cursor, "select last_insert_rowid()" + table_name)

    def _cursor(self):
        return self.db_object.cursor()

    @staticmethod
    @many_try(max_try=100, error_got="database is locked", sleep_time=1,
              error_manager_key="BD_SQLITE")
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
        args = [script]
        if params is None:
            pass
        elif isinstance(params, (tuple, list)):
            if len(params):
                if isinstance(params[0], dict):
                    final_res = []
                    for p in params:
                        temp = {}
                        for k in p:
                            if ":" + str(k) not in script:
                                if not isinstance(temp, dict):
                                    temp.append(p[k])
                                else:
                                    temp[k] = p[k]
                                    temp = list(temp.values())
                            else:
                                if isinstance(temp, dict):
                                    temp[k] = p[k]
                                else:
                                    temp.append(p[k])
                        final_res.append(temp)
                    params = final_res
                params = tuple(params)
                args.append(params)
        elif isinstance(params, dict):
            if len(params):
                k = list(params.keys())[0]
                if ":" + str(k) in script:
                    pass
                else:
                    params = list(params.values())
                args.append(params)
        else:
            params = (params,)
            args.append(params)
        try:

            getattr(cursor, method)(*args)
            return cursor
        except Exception as ex:
            # print(params, script)
            if ignore_error:
                return None
            raise ex

    @staticmethod
    def _get_cursor_description(cursor):
        return Cdict(columns=[desc[0] for desc in cursor.description or []])

    @staticmethod
    def prepare_insert_data(data: dict):
        return ["?" for _ in data], list(data.values())

    def dump(self, dump_file='dump.sql'):
        self.reload_connexion()
        with open(dump_file, 'w') as f:
            for line in self.db_object.iterdump():
                f.write('%s\n' % line)


if __name__ == '__main__':
    db_object = SQLiteDB(file_name=None)

    db_object.create_table(r"C:\Users\FBYZ6263\Downloads\SWAP_FACEBOOK1.0.csv",
                           auto_increment_field=True)
    print(db_object.run_script("SELECT * FROM new_table", limit=5))
