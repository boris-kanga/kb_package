import abc
import re
import traceback
import typing

from pandas.core.dtypes.common import is_numeric_dtype, is_object_dtype

from kb_package.tools import INFINITE
import os
from kb_package import tools


class BaseDB(abc.ABC):
    MYSQL_DEFAULT_PORT = 3306
    DEFAULT_PORT = None
    REGEX_SPLIT_URI = re.compile(r"(\w+?)://([\w\-.]+):([\w\-.]+)@"
                                 r"([\w\-.]+):(\d+)(?:/([\w\-.]+))?")
    LAST_SQL_CODE_RUN = None

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
            self.username = uri.get("user", "root")
            self.password = uri.get("pwd", "") or uri.get("password", "")
            self.host = uri.get("host", "127.0.0.1")
            self.port = uri.get("port", self.DEFAULT_PORT)
            self.database_name = uri.get("db_name")
            self.file_name = uri.get("file_name") or ":memory:"
        else:
            assert isinstance(uri, str), "Bad URI value given"
            if os.path.exists(uri):
                self.file_name = uri
                self.username, self.password, self.host, \
                    self.port, self.database_name = None, None, None, None, None

            else:
                res = re.match(self.REGEX_SPLIT_URI, uri)
                self.file_name = ":memory:"
                if not res:
                    raise ValueError("Got bad uri")
                _, self.username, self.password, self.host, \
                    self.port, self.database_name = res.groups()
            uri = {}

        self._kwargs = uri

        self._kwargs.update({
            "host": self.host,
            "user": self.username,
            "password": self.password,
            "db_name": self.database_name,
            "port": self.port, "file_name": self.file_name
        })

        self.communicate_info = print
        self.communicate_error = print

        self.db_object = None
        self._cursor_ = None

    def __enter__(self):
        self.reload_connexion()
        self._cursor_ = self._cursor()
        return self._cursor_

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._cursor_.close()
        finally:
            self._cursor_ = None
            self.close_connection()

    def set_logger(self, logger):
        if hasattr(logger, "info"):
            self.communicate_info = logger.info
        if hasattr(logger, "error"):
            self.communicate_info = logger.error
        if hasattr(logger, "exception"):
            self.communicate_info = logger.exception

        if callable(logger):
            self.communicate_info = logger
            self.communicate_error = logger

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
    def prepare_insert_data(data: list):
        return ["%s" for _ in data]

    def insert(self, value, table_name, cur=None, retrieve_id=False):
        part_vars = [str(k) for k in value.keys()]
        value = [v if v is not None else "null" for v in value.values()]
        script = "INSERT INTO " + str(table_name) + \
                 " ( " + ",".join(part_vars) + \
                 ") VALUES ( " + ", ".join(self.prepare_insert_data(value)) + " ) "

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

    def insert_many(self, data, table_name):
        if self._cursor_:
            cursor = self._cursor_
        else:
            cursor = self.get_cursor()
        for d in data:
            self.insert(d, table_name, cursor)
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
        except AttributeError:
            pass

    @staticmethod
    @abc.abstractmethod
    def _execute(cursor, script, params=None, ignore_error=False,
                 connexion=None):
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
            self.communicate_error(ex)

            if not ignore_error:
                raise Exception(ex)

        self.commit()
        if retrieve:
            return self.get_all_data_from_cursor(cursor, limit=limit, dict_res=dict_res)

    @staticmethod
    def get_add_increment_field_code(field_name="id"):
        return str(field_name or "id") + " INTEGER PRIMARY KEY AUTOINCREMENT"

    def create_table(self, arg: str, table_name=None, auto_increment_field=False, auto_increment_field_name=None):
        with self:
            if os.path.exists(arg):
                dataset = tools.read_datafile(arg)
                if not len(dataset.columns):
                    return
                table_name = tools.format_var_name(table_name or "new_table")
                table_script = f"CREATE TABLE IF NOT EXISTS {table_name}("
                if auto_increment_field:
                    table_script += "\n\t" + self.get_add_increment_field_code(auto_increment_field_name) + ","
                equivalent = {}
                for index, col in enumerate(dataset.columns):
                    field = tools.format_var_name(col)
                    equivalent[col] = field
                    if index > 0:
                        table_script += ","
                    if is_numeric_dtype(dataset[col]):
                        table_script += f"\n\t{field} int"
                    elif is_object_dtype(dataset[col]):
                        size = dataset[col].apply(lambda x: len(x)).max()
                        if size > 1024 * 1024:
                            type_ = 'text'
                        else:
                            type_ = f"varchar({size or 255})"
                        table_script += f"\n\t{field} {type_}"
                table_script += "\n)"
                print(table_script)
                self.run_script(table_script, retrieve=False)
                data = []
                max_buffer_size = 200
                for _, row in dataset.iterrows():
                    data.append({field: row[col] for col, field in equivalent.items()})
                    if len(data) >= max_buffer_size:
                        self.insert_many(data, table_name=table_name)
                        data = []
                print("Finish")
            else:
                self.run_script(arg)

    def dump(self, dump_file='dump.sql'):
        pass
