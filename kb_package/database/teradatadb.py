# -*- coding: utf-8 -*-
"""
The Teradata database manager.
Use for run easily Teradata requests
"""
from kb_package.tools import INFINITE
from kb_package.database.basedb import BaseDB, for_csv
import teradatasql


class TeradataDB(BaseDB):
    DEFAULT_PORT = 1025

    @property
    def _get_name(self):
        return self.__class__.__name__

    def _is_connected(self):
        return True

    @staticmethod
    def connect(host="127.0.0.1", user="root", password="", db_name=None, port=DEFAULT_PORT, **kwargs):
        """
        Making the connexion to the mysql database

        Args:
            user
            host
            password
            db_name
            port
        Returns: the connexion object reach

        """
        try:
            return teradatasql.connect(host=host, user=user, password=password, database=db_name, dbs_port=str(port))
        except Exception as ex:
            ex.args = ["Une erreur lors que la connexion à la base de donnée --> " + str(ex.args[0])] + \
                      list(ex.args[1:])
            raise ex

    def _cursor(self):
        return self.db_object.cursor()

    @staticmethod
    def prepare_insert_data(data: dict):
        return ["?" for _ in data], list(data.values())

    @staticmethod
    def _execute(cursor, script, params=None, ignore_error=False, method="single", **kwargs):
        TeradataDB.LAST_SQL_CODE_RUN = script
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
            raise Exception(ex)

    @staticmethod
    def get_all_data_from_cursor(cursor, limit=INFINITE, dict_res=False, export_name=None, sep=";"):
        columns = [col[0] for col in cursor.description or []]
        TeradataDB.LAST_REQUEST_COLUMNS = columns
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
                    if dict_res and export_name is None:
                        row = dict(zip(columns, row))
                    if export_name is not None:
                        export_file.write(for_csv(row, sep=sep) + "\n")
                    else:
                        data.append(row)
                    index_data += 1
            if export_name is not None:
                return
        except (Exception, teradatasql.DatabaseError):
            pass
        if limit == 1:
            if len(data):
                return data[0]
            return None
        return data

    def create_table(self, arg, table_name=None, if_not_exists=True,
                     auto_increment_field=False,
                     auto_increment_field_name=None,
                     columns=None, ftype=None, verbose=True, only_structure=False, **kwargs):
        if isinstance(arg, str):
            super().create_table(arg, table_name=table_name, if_not_exists=if_not_exists,
                                 auto_increment_field=auto_increment_field,
                                 auto_increment_field_name=auto_increment_field_name,
                                 columns=columns, ftype=ftype, verbose=verbose, only_structure=True, **kwargs)

            if only_structure:
                return
            self.insert_many(arg, table_name=table_name, loader=kwargs.get("loader"))
        else:
            super().create_table(arg, table_name=table_name, if_not_exists=if_not_exists,
                                 auto_increment_field=auto_increment_field,
                                 auto_increment_field_name=auto_increment_field_name,
                                 columns=columns, ftype=ftype, verbose=verbose, **kwargs)

    def insert_many(self, data, table_name, verbose=True, ftype=None,
                    ignore_type=False,
                    **kwargs):
        # for export
        # self._cursor().execute ("{fn teradata_write_csv(" + sFileName + ")}select * from table")
        if isinstance(data, str):
            self._cursor().execute("{fn teradata_read_csv(%s)} insert into %s (?, ?)" % (data, table_name))
        else:
            super().insert_many(data, table_name, verbose=verbose, ftype=ftype, ignore_type=ignore_type, **kwargs)
