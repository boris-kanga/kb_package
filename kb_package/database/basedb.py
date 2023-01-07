from __future__ import annotations

import abc
import re
import traceback
import typing

import pandas
from pandas.api.types import is_float_dtype, is_integer_dtype
from kb_package.tools import INFINITE
import os
from kb_package import tools
from kb_package.utils.fdataset import DatasetFactory
from datetime import datetime as _datetime


def _is_datetime_field(value):
    try:
        d = tools.CustomDateTime(value)()
        return pandas.isnull(d) or isinstance(d, _datetime)
    except (AssertionError, Exception):
        return False


class BaseDB(abc.ABC):
    MYSQL_DEFAULT_PORT = 3306
    DEFAULT_PORT = None
    REGEX_SPLIT_URI = re.compile(r"(\w+?)://([\w\-.]+):([\w\-.]+)@"
                                 r"([\w\-.]+):(\d+)(?:/([\w\-.]+))?")
    LAST_SQL_CODE_RUN = None
    MAX_BUFFER_INSERTING_SIZE = 5000
    LAST_REQUEST_COLUMNS = None

    def __init__(self, uri=None, **kwargs):
        """
            kwargs:
                user: username
                pwd | password: the connexion password
                port: default 3306 (the MySQL default port)
                host: default localhost
        """

        if uri is None:
            uri = kwargs
        if isinstance(uri, dict):
            uri = {k.lower(): uri[k] for k in uri}
            self.username = uri.get("user", "root")
            self.password = uri.get("pwd", "") or uri.get("password", "")
            self.host = uri.get("host", "127.0.0.1")
            self.port = uri.get("port", self.DEFAULT_PORT)
            self.database_name = uri.get("db_name")
            self.file_name = uri.get("file_name") or ":memory:"
        else:
            assert isinstance(uri, str), "Bad URI value given"
            res = re.match(self.REGEX_SPLIT_URI, uri)
            if res:
                self.file_name = ":memory:"
                _, self.username, self.password, self.host, \
                    self.port, self.database_name = res.groups()

            else:
                self.file_name = uri
                self.username, self.password, self.host, \
                    self.port, self.database_name = None, None, None, None, None
            uri = {}

        self._kwargs = uri

        self._kwargs.update({
            "host": self.host,
            "user": self.username,
            "password": self.password,
            "db_name": self.database_name,
            "port": self.port, "file_name": self.file_name
        })
        self._print_info = print
        self._print_error = print
        self.set_logger(self._kwargs.get("logger"))

        self.db_object = None
        self._cursor_ = None

    @property
    def _get_name(self):
        return self.__class__.__name__

    def __enter__(self):
        self.reload_connexion()
        self._cursor_ = self._cursor()
        return self._cursor_

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._cursor_.close()
        except (AttributeError, Exception):
            pass
        finally:
            self._cursor_ = None
            self.close_connection()

    def set_logger(self, logger):
        if hasattr(logger, "info"):
            self._print_info = logger.info
        if hasattr(logger, "error"):
            self._print_error = logger.error

        if callable(logger):
            self._print_info = logger
            self._print_error = logger

    @staticmethod
    @abc.abstractmethod
    def connect(
            host="127.0.0.1", user="root", password="", db_name=None,
            port=DEFAULT_PORT, file_name=None, **kwargs
    ):
        """
        Making the connexion to the mysql database
        Args:
            host: str, the Mysql server ip
            user: str, the username for mysql connexion
            password: str, the password
            db_name: str, the database name
            port: int,
            file_name:

        Returns: the connexion object reach

        """
        pass

    def reload_connexion(self):
        """
        initialize new connexion to the mysql database

        Returns:

        """
        self.close_connection()
        self.db_object = self.connect(**self._kwargs)

    @abc.abstractmethod
    def _cursor(self):
        return None

    def last_insert_rowid_logic(self, cursor=None, table_name=None):
        return cursor

    @staticmethod
    def prepare_insert_data(data: dict):
        return ["%s" for _ in data], data

    def insert(self, value: dict, table_name, cur=None, retrieve_id=False):
        part_vars = [str(k) for k in value.keys()]

        xx, value = self.prepare_insert_data(value)

        # value = [v if v is not None else "null" for v in value.values()]
        script = "INSERT INTO " + str(table_name) + \
                 " ( " + ",".join(part_vars) + \
                 ") VALUES ( " + ", ".join(xx) + " ) "

        if cur is None:
            if self._cursor_:
                cursor = self._cursor_
            else:
                cursor = self.get_cursor()
        else:
            cursor = cur
        return_object = cursor
        self._execute(cursor, script, params=value)
        if retrieve_id:
            cursor = self.last_insert_rowid_logic(cursor, table_name)
            return_object = self.get_all_data_from_cursor(cursor, limit=1)
            if isinstance(return_object, (list, tuple)):
                return_object = 0 if not len(return_object) else return_object[0]
        if cur is None and not self._cursor_:
            self.commit()
        return return_object

    # @abc.abstractmethod
    def db_types(self):
        return "SQL"

    def insert_many(self, data: typing.Union[list, pandas.DataFrame, str], table_name, verbose=True, ftype=None,
                    ignore_type=False,
                    **kwargs):
        print = self._print_info
        loader = kwargs.pop("loader", None)
        if self._cursor_:
            cursor = self._cursor_
        else:
            cursor = self.get_cursor()

        dataset = DatasetFactory(data, **kwargs).dataset
        size = dataset.shape[0]
        if not size:
            return
        # MAX_BUFFER_INSERTING_SIZE
        if not isinstance(ftype, dict):
            if len(dataset.columns) == 1 and isinstance(ftype, str):
                ftype = {dataset.columns[0]: ftype}
            else:
                ftype = {}
        types = {}
        for field in dataset.columns:
            types[field] = lambda x: x
            if not ignore_type:
                if is_integer_dtype(dataset[field]) and (
                        is_integer_dtype(str(ftype.get(field)).lower()) or ftype.get(field) is None):
                    types[field] = int
                elif (is_float_dtype(dataset[field]) or is_integer_dtype(dataset[field])) and (
                        is_float_dtype(str(ftype.get(field)).lower()) or ftype.get(field) is None):
                    types[field] = float

        dataset = dataset.to_dict("records")
        first_value = dataset[0]
        part_vars = [str(k) for k in first_value.keys()]

        xx, value = self.prepare_insert_data(first_value)

        # value = [v if v is not None else "null" for v in value.values()]
        script = "INSERT INTO " + str(table_name) + \
                 " ( " + ",".join(part_vars) + \
                 ") VALUES ( " + ", ".join(xx) + " ) "
        if verbose:
            tools.ConsoleFormat.progress(0)
        for t, buffer in tools.get_buffer(dataset, max_buffer=self.MAX_BUFFER_INSERTING_SIZE):
            try:
                self._execute(cursor, script,
                              params=[
                                  {
                                      k: types[k](v)
                                      if not pandas.isnull(v) else None
                                      for k, v in row.items()
                                  } for row in buffer], method="many")
                if callable(loader):
                    loader(t)
                if verbose:
                    tools.ConsoleFormat.progress(100 * t)
            except Exception as ex:
                # print("\n", "->Got error with the buffer: ", buffer)
                traceback.print_exc()
                self._print_error(ex)
                return
        if verbose:
            print("... Finish ...")
        self.commit()

    def get_cursor(self):
        """
        Get mysql cursor for making requests
        Returns: mysql.connector.cursor.MySQLCursor, mysql cursor for requests

        """
        try:
            return self._cursor()
        except (Exception, AttributeError):
            self.reload_connexion()
            return self._cursor()

    def close_connection(self):
        """
        close the last connexion establish
        Returns: None

        """
        try:
            self.db_object.close()
        except (AttributeError, Exception):
            pass

    @staticmethod
    @abc.abstractmethod
    def _execute(cursor, script, params=None, ignore_error=False,
                 connexion=None, method="single", **kwargs):
        """
        use to make preparing requests
        Args:
            cursor: cursor object
            script: str, the prepared requests
            params: list|tuple|dict, params for the mysql prepared requests
            ignore_error: bool, if ignore error
            connexion:

        Returns: cursor use for request

        """

    def commit(self):
        try:
            self.db_object.commit()
        except (Exception, AttributeError):
            pass

    @staticmethod
    @abc.abstractmethod
    def get_all_data_from_cursor(cursor, limit=INFINITE, dict_res=False):
        return []

    def run_script(self, script: typing.Union[list, str], params=None, retrieve=True, limit=INFINITE,
                   ignore_error=False, dict_res=False):
        """
        Run a specific sql file
        Args:
            script: str
            params: list|tuple|dict, params for the mysql prepared requests
            retrieve: bool, for select requests;
            limit: int nb of data to retrieve if retrieve
            ignore_error: to ignore or raise error if an error happened
            dict_res: bool, return result as dict args

        Returns: data results if retrieve

        """
        self.LAST_SQL_CODE_RUN = script
        if self._cursor_ is not None:
            cursor = self._cursor_
        else:
            cursor = self.get_cursor()
        try:
            if isinstance(script, str):
                script = [script]
            for s in script:
                cursor = self._execute(cursor, s, params=params,
                                       ignore_error=False,
                                       connexion=self.db_object)
        except Exception as ex:
            print("*" * 10, "Got error when try to run", "*" * 10)
            print(self.LAST_SQL_CODE_RUN)
            print("**" * 10)
            traceback.print_exc()
            self._print_error(ex)

            if not ignore_error:
                raise Exception(ex)

        self.commit()
        if retrieve:
            data = self.get_all_data_from_cursor(cursor, limit=limit, dict_res=dict_res)
            if dict_res:
                if limit == 1:
                    return tools.Cdict(data)
                return [tools.Cdict(d) for d in data]
            return data

    @staticmethod
    def get_add_increment_field_code(field_name="id"):
        return str(field_name or "id") + " INTEGER PRIMARY KEY AUTOINCREMENT"

    def create_table(self, arg: str | pandas.DataFrame, table_name=None, if_not_exists=True,
                     auto_increment_field=False,
                     auto_increment_field_name=None,
                     columns=None, ftype=None, verbose=True, **kwargs):
        if verbose:
            print = self._print_info
        else:
            print = lambda *args: None
        with self:
            if isinstance(arg, pandas.DataFrame):
                dataset = arg
            elif isinstance(arg, str) and os.path.exists(arg):
                dataset = DatasetFactory(arg, **kwargs).dataset

            elif isinstance(arg, str):
                self.run_script(arg)
                return
            else:
                raise TypeError("Bad value of arg given: %s" % arg)
        if not len(dataset.columns):
            return
        if not isinstance(ftype, dict):
            ftype = {}
        if columns is not None:
            if isinstance(columns, list):
                dataset = dataset.loc[:, columns]
            elif isinstance(columns, dict):
                dataset = dataset.loc[:, columns.keys()]
                dataset.rename(columns=columns, inplace=True)
        print("got %s data from file given" % dataset.shape[0])
        print(dataset)
        table_name = tools.format_var_name(table_name or "new_table")
        table_script = f"CREATE TABLE " \
                       f"{'IF NOT EXISTS' if (if_not_exists and 'oracle' not in self._get_name.lower()) else ''} " \
                       f"{table_name}("
        if auto_increment_field:
            table_script += "\n\t" + self.get_add_increment_field_code(auto_increment_field_name) + ","
        equivalent = {}
        print("Going to create table structure")
        for index, col in enumerate(dataset.columns):
            field = tools.format_var_name(col) or "field" + str(index)

            equivalent[col] = field
            if index > 0:
                table_script += ","

            if is_integer_dtype(dataset[col]) and (
                    is_integer_dtype(str(ftype.get(col)).lower()) or ftype.get(col) is None):
                table_script += f"\n\t{field} int"
            elif (is_float_dtype(dataset[col]) or is_integer_dtype(dataset[col])) and (
                    is_float_dtype(str(ftype.get(col)).lower()) or ftype.get(col) is None):
                table_script += f"\n\t{field} float"
            else:
                got = False
                if ftype.get(col) is not None:
                    got = True
                    table_script += f"\n\t{field} {ftype.get(col)}"

                elif dataset[col].apply(lambda val: not _is_datetime_field(val)).any() and (
                        "date" not in str(ftype.get(col)).lower() or ftype.get(col) is None
                ):
                    dataset[col] = dataset[col].apply(lambda x: str(x) if not pandas.isnull(x) else None)
                    size = dataset[col].apply(lambda x: len(x)).max()
                    if size > 255:
                        type_ = 'text'
                    else:
                        if size > 3:
                            size = max(255, size)
                        type_ = f"varchar({size or 255})"

                else:
                    if dataset[col].apply(lambda val: tools.CustomDateTime(str(val)).is_datetime).any():
                        type_ = "datetime"
                        dataset[col] = dataset[col].apply(
                            lambda val: tools.CustomDateTime(str(val), default=None)())
                    else:
                        dataset[col] = dataset[col].apply(
                            lambda val: tools.CustomDateTime(str(val), default=None).date)
                        type_ = "date"
                if not got:
                    table_script += f"\n\t{field} {type_}"
            dataset.rename(columns={col: field}, inplace=True)

        table_script += "\n)"
        if if_not_exists and "oracle" in self._get_name.lower():
            try:
                self.run_script("select * from " + table_name, limit=1)
                print("The Table specify were exists: Going to drop it")
                self.run_script("drop table " + table_name, retrieve=False)
                self.commit()
            except:
                pass

        print("Going to create table with the query:", table_script)
        self.run_script(table_script, retrieve=False)

        # dataset.to_sql(con=self.db_object)
        # INSERTING DATASET
        self.insert_many(dataset, table_name=table_name, loader=kwargs.get("loader"))

    def dump(self, dump_file='dump.sql'):
        pass
