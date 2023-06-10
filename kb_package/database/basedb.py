from __future__ import annotations

import abc
import csv
import re
import traceback
import typing

import pandas
from pandas.api.types import is_float_dtype, is_integer_dtype
from kb_package.tools import INFINITE
import os
from kb_package import tools
from kb_package.utils.fdataset import DatasetFactory


def _parse_date_value(value, format_=()):
    default_date_format = ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%d_%m_%Y", "%Y_%m_%d",
                           "%d %B %Y", "%d-%B-%Y")
    if not format_:
        format_ = default_date_format
    elif isinstance(format_, str):
        format_ = list(default_date_format) + [format_]
    if pandas.isnull(value):
        return None, False
    value = tools.CustomDateTime(str(value), d_format=format_)
    return value.to_string("yyyy-mm-dd hh:mm:ss"), value.is_datetime


def _can_be_datetime_field(series, date_format=()):
    # returns NO, DATETIME, DATE
    try:
        for ss in tools.get_buffer(series, 10000, vv=False):
            res = ss.apply(lambda val: _parse_date_value(val, format_=date_format))
            if any([d[1] for d in res]):
                return "DATETIME"
            if len([... for d in res if d[0] is not None]) >= len(ss)/3:
                return "DATE"
        return "DATE"
    except (ValueError, AssertionError, Exception):
        return "NO"


class BaseDB(abc.ABC):
    MYSQL_DEFAULT_PORT = 3306
    DEFAULT_PORT = None
    REGEX_SPLIT_URI = re.compile(r"(\w+?)://([\w\-.]+):([\w\-.]+)@"
                                 r"([\w\-.]+):(\d+)(?:/([\w\-.]+))?")
    LAST_SQL_CODE_RUN = None
    MAX_BUFFER_INSERTING_SIZE = 100000
    LAST_REQUEST_COLUMNS = None
    LAST_RUN_SCRIPT_ERROR = None

    MAX_PARAMETERS = 1200

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
    @tools.many_try(max_try=1, sleep_time=0, error_manager_key="BD_BASE")
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
        ...

    def reload_connexion(self):
        """
        initialize new connexion to the mysql database

        Returns:

        """
        self.close_connection()
        self.db_object = self.connect(**self._kwargs)

    @abc.abstractmethod
    def _cursor(self):
        ...

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
        if self._get_name == "PostgresDB" and retrieve_id:
            script += " RETURNING id"
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
            if self._get_name == "MysqlDB":
                return_object = cursor.lastrowid
            else:
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

    def insert_many(self, data: typing.Union[list, pandas.DataFrame, str], table_name, verbose=False,
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
        # dataset = dataset.to_dict("records")
        first_value = dataset.loc[0].to_dict()
        part_vars = [str(k) for k in first_value.keys()]

        xx, _ = self.prepare_insert_data(first_value)

        script = "INSERT INTO " + str(table_name) + \
                 " ( " + ",".join(part_vars) + \
                 ") VALUES ( " + ", ".join(xx) + " ) "
        if verbose:
            print("Inserting ...")
            tools.ConsoleFormat.progress(0)
        for t, buffer in tools.get_buffer(dataset, max_buffer=self.MAX_BUFFER_INSERTING_SIZE):
            buffer = buffer.astype(object).replace(DatasetFactory.NAN, None).to_dict("records")
            try:
                self._execute(cursor, script,
                              params=[
                                  {
                                      k: v
                                      if not pandas.isnull(v) else None
                                      for k, v in row.items()
                                  } for row in buffer], method="many")
                if callable(loader):
                    loader(t)
                if verbose:
                    tools.ConsoleFormat.progress(100 * t)
                self.commit()
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

    def _set_last_exception_after_execute(self, last_script_part, exception, min_line=0, params=None):
        self.LAST_RUN_SCRIPT_ERROR = tools.Cdict(msg=str(exception).split("\n")[0],
                                                 min_line=min_line,
                                                 max_line=min_line + last_script_part.count("\n"),
                                                 params=params)
        if hasattr(exception, "offset"):
            line = last_script_part[:int(exception.offset)].count("\n") + min_line
            self.LAST_RUN_SCRIPT_ERROR.script = last_script_part
            self.LAST_RUN_SCRIPT_ERROR.offset = exception.offset
            self.LAST_RUN_SCRIPT_ERROR.error_line = line

    @staticmethod
    @abc.abstractmethod
    @tools.many_try(max_try=1, sleep_time=0, error_manager_key="BD_BASE")
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

    def _is_connected(self):
        return True

    def commit(self):
        try:
            self.db_object.commit()
        except (Exception, AttributeError):
            pass

    @staticmethod
    def _fetchone(cursor, limit=INFINITE):
        index_data = 0
        while index_data < limit:
            row = cursor.fetchone()
            if not row:
                break
            index_data += 1
            yield row

    @staticmethod
    @abc.abstractmethod
    def _get_cursor_description(cursor):
        ...

    @staticmethod
    def _check_if_cursor_has_rows(cursor):
        return True

    # @abc.abstractmethod
    def get_all_data_from_cursor(self, cursor, limit=INFINITE, dict_res=False, export_name=None, sep=";"):
        try:
            self.LAST_REQUEST_COLUMNS = None
            if not self._check_if_cursor_has_rows(cursor):
                return None
            self.LAST_REQUEST_COLUMNS = self._get_cursor_description(cursor).columns
            columns = self.LAST_REQUEST_COLUMNS
            assert columns is not None
        except (AssertionError, Exception):
            return None

        data = []
        try:
            export_file = type("MyTempFile", (), {"__enter__": lambda *args: 1, "__exit__": lambda *args: 1})()
            if callable(export_name):
                pass
            elif export_name is not None:
                # export_file = open(export_name, "w")
                export_file = open(export_name, "w", newline="")

            with export_file:
                writer = None
                if export_name is not None and not callable(export_name):
                    writer = csv.writer(export_file, delimiter=sep)
                    writer.writerow(columns)
                    # export_file.write(for_csv(columns, sep=sep) + "\n")

                for row in self._fetchone(cursor, limit=limit):
                    if not row:
                        break
                    if dict_res and export_name is None:
                        row = dict(zip(columns, row))
                    if callable(export_name):
                        export_name(row, columns)
                    elif export_name is not None:
                        writer.writerow(row)
                        # export_file.write(for_csv(row, sep=sep) + "\n")
                    else:
                        data.append(row)
            if export_name is not None:
                return
        except Exception:
            # traceback.print_exc()
            pass
        if limit == 1:
            if len(data):
                return data[0]
            return None
        return data

    @staticmethod
    def _script_tokenizer(sql):
        # prepare sql code
        sql = BaseDB._uncomment_sql(sql, sigle_line_symbole="--")

        sql_without_quote, quotes_ref = tools.replace_quoted_text(sql)
        # sql_without_quote = sql_without_quote.strip()
        if not sql_without_quote.endswith(";"):
            sql_without_quote += ";"

        # check for delimiters (mysql delimiter [\W+|//] ... end [\W+|//] )

        script_without_proc, proc_refs = tools.extract_structure(
            sql_without_quote,
            symbol_start=r"^\s*(?:(?:PROCEDURE\s.+?|DECLARE.+?|"
                         r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE|TRIGGER)\s.+?)"
                         r"^\s*BEGIN|BEGIN)",
            symbol_end=r";\s*^\s*END\s*;(?:\s*/)?",
            maximum_deep=1, flags=re.S | re.I | re.M, sep=";")

        # spec case of PACKAGE BODY
        script, package_ref = tools.extract_structure(
            script_without_proc,
            symbol_start=r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+BODY\s+(?:\w+.)(\w+)\s+(?:IS|AS)",
            symbol_end=";\\s*^\\s*END\\s+\1\\s*;(?:^\\s*/)?",
            maximum_deep=1, flags=re.S | re.I | re.M, sep=";"
        )
        queries = []
        for query in script.split(";"):
            # recalculation
            for k in package_ref:
                query = query.replace(k, package_ref[k], 1)
            for k in proc_refs:
                query = query.replace(k, proc_refs[k], 1)
            for k in quotes_ref:
                query = query.replace(k, quotes_ref[k], 1)

            # query = query.strip()
            if len(query.strip()):
                queries.append(query)
        return queries

    @staticmethod
    def _uncomment_sql(script, sigle_line_symbole="--", show_comments_func=None):
        if sigle_line_symbole is None:
            sigle_line_symbole = ["--", "#"]
        elif isinstance(sigle_line_symbole, str):
            sigle_line_symbole = [sigle_line_symbole]
        sigle_line_symbole = "|".join(sigle_line_symbole)
        final_script = ""

        comments_list = []
        # multiline comment remove
        query, r = tools.replace_quoted_text(script)
        query, comments = tools.extract_structure(
            query,
            symbol_start=r"/\*",
            symbol_end=r"\*/",
            maximum_deep=1, flags=re.S
        )
        for k in comments:
            com = comments[k]
            for _str in r:
                com = com.replace(_str, r[_str], 1)
            query = query.replace(k, "\n" * (com.count("\n")), 1)
        comments_list.extend(list(comments.values()))
        qquery = query.split("\n")
        final_script = []
        for line in qquery:
            line = re.split(sigle_line_symbole, line, maxsplit=1)
            final_script.append(line[0])
            comments_list.extend(line[1:])
        final_script = "\n".join(final_script)
        for k in r:
            final_script = final_script.replace(k, r[k], 1)
            for i in range(len(comments_list)):
                comments_list[i] = comments_list[i].replace(k, r[k], 1)
        if callable(show_comments_func):
            show_comments_func("Got comments: " + repr(comments_list))
        return final_script

    @staticmethod
    def _prepare_query(sql, params=None, ignore_error=False):
        query, r = tools.replace_quoted_text(sql)
        origin_transform_query = query
        final_params = {}
        # 1 for args like ":var"; 0 for ?
        query_prepare_type = 1
        nb_var = 0
        got = False
        code = ""
        if "?" in query or "%s" in query:
            got = True
            final_params = []
            query_prepare_type = 0
            if isinstance(params, dict):
                params = list(params.values())
            elif isinstance(params, (list, tuple)):
                params = list(params)
            else:
                params = []
            res = re.search(r"(\sin\s*)?(\?|%s)", query)
            i = 0
            code = ""

            while res:
                nb_var += 1
                code += query[:res.span()[0]]
                query = query[res.span()[1]:]
                try:
                    pp = params[i]
                except IndexError:
                    if not ignore_error:
                        raise ValueError(
                            "Prepared args " + (str(i + 1)) + " in the script is not specify:" + repr(sql))
                    pp = None
                i += 1
                _groups = res.groups()
                if _groups[0]:
                    code += " in "
                    if tools.BasicTypes.is_iterable(pp) or pp is None:
                        pp = list(pp) or [None] if pp is not None else [None]
                        final_params.extend(pp)
                        code += "(" + (','.join([_groups[1] for _ in pp])) + ")"
                    else:
                        final_params.append(pp)
                        code += _groups[1]
                else:
                    code += _groups[1]
                    final_params.append(pp)
                # relance
                res = re.search(r"(\sin\s*)?(\?|%s)", query)
            code += query
        elif re.search(r"(:\w+\W|%\(\w+\)s\W)", query + " "):
            if params is None:
                params = {}
            got = True
            code = ""
            res = re.search(r"(\sin\s*)?(:(\w+)|%\((\w+)\)s)(\W)", query + " ")
            global_params = os.environ.copy()
            global_params.update(params)
            global_params = tools.Cdict(global_params)
            while res:
                nb_var += 1
                code += query[:res.span()[0]]
                query = query[res.span()[1]:]
                struc, i, ii, sep = res.groups()[1:]
                i = i or ii
                try:
                    assert i in global_params
                    pp = global_params[i]
                except (KeyError, Exception, AttributeError):
                    if not ignore_error:
                        raise ValueError("Prepared args (" + i + ") in the script is not specify:" + repr(sql))
                    pp = None

                if res.groups()[0]:
                    code += " in "
                    if tools.BasicTypes.is_iterable(pp) or pp is None:
                        pp = list(pp) or [None] if pp is not None else [None]
                        code += "("
                        if i in final_params:
                            in_params = final_params[i]
                            for index in in_params.keys():
                                if struc.startswith(":"):
                                    code += ":" + index + ","
                                else:
                                    code += "%(" + index + ")s,"
                        else:
                            in_params = {}
                            for e in pp:
                                index = tools._get_new_kb_text(origin_transform_query + code, "in_elem")
                                in_params[index] = e
                                if struc.startswith(":"):
                                    code += ":" + index + ","
                                else:
                                    code += "%(" + index + ")s,"
                            code = code[:-1]
                            code += ")" + sep
                            final_params[i] = in_params
                    else:
                        final_params[i] = pp
                        if struc.startswith(":"):
                            code += ":" + i + sep
                        else:
                            code += "%(" + i + ")s" + sep
                else:
                    final_params[i] = pp
                    if struc.startswith(":"):
                        code += ":" + i + sep
                    else:
                        code += "%(" + i + ")s" + sep
                # relance
                res = re.search(r"(\sin\s*)?(:(\w+)|%\((\w+)\)s)(\W)", query + " ")
            code += query

            format_params = {}
            for k, v in final_params.items():
                if isinstance(v, dict):
                    format_params.update(v)
                else:
                    format_params[k] = v
            final_params = format_params
        if got:
            for k in r:
                code = code.replace(k, r[k], 1)
        else:
            code = sql
        return code, final_params, query_prepare_type, nb_var

    @staticmethod
    def _get_sql_type(script):
        res = re.search(r"^(?:[(\s]*)?(delete|select|insert|update|with|merge|create|replace|alter"
                        r"|commit|truncate|call|rename|[a-z]+)",
                        script.strip(), flags=re.I)
        if res:
            return res.groups()[0]
        return "unknown"

    @staticmethod
    def __test__(script, params=None):
        if isinstance(script, str):
            try:
                assert os.path.exists(script)
                with open(script) as file:
                    script = file.read().strip()
            except (AssertionError, OSError, Exception):
                pass
            script = BaseDB._script_tokenizer(script)
        for s in script:
            s, consider_params, _type, nb_var = BaseDB._prepare_query(s, params)
            print(s, consider_params, _type, nb_var)

    def run_script(self, script: typing.Union[list, str], params=None, *, retrieve=None, limit=INFINITE,
                   ignore_error=False, dict_res=False, export=False, export_name=None, sep=";", timeout=None):
        """
        Run a specific sql file
        Args:
            script: str, or path to the sql code
            params: list|tuple|dict, params for the mysql prepared requests
            retrieve: bool, for select requests;
            limit: int nb of data to retrieve if retrieve
            ignore_error: to ignore or raise error if an error happened
            dict_res: bool, return result as dict args
            export: bool, if it's necessary to export te data
            export_name: (str) the file name
            sep: csv separator for export
            timeout: float, nb of seconds for maximum time of execution

        Returns: data results if retrieve

        """
        self.LAST_RUN_SCRIPT_ERROR = None
        if timeout is not None and str(self._get_name).lower() == "sqlitedb" and self.file_name == ":memory:":
            raise ValueError("Bad argument timeout set. for SQLiteDB impossible to set timeout. Do it yourself")
        if timeout is not None:
            with tools.thread.ThreadPoolExecutor() as executor:
                proc = executor.submit(self.run_script, script,
                                       **{"params": params, "retrieve": retrieve,
                                          "limit": limit, "ignore_error": ignore_error, "dict_res": dict_res,
                                          "export": export, "export_name": export_name, "sep": sep,
                                          "timeout": None})
                try:
                    return proc.result(timeout=timeout)
                except tools.thread.TimeoutError as timeout_ex:
                    proc.cancel()
                    executor.shutdown(wait=False, cancel_futures=True)
                    self.rollback()
                    raise timeout_ex

        if isinstance(script, str):
            try:
                assert os.path.exists(script)
                with open(script) as file:
                    script = file.read().strip()
            except (AssertionError, OSError, Exception):
                pass
            script = self._script_tokenizer(script)

        self.LAST_SQL_CODE_RUN = ";".join(script)
        if limit is None:
            limit = INFINITE
        if not self._is_connected():
            self.reload_connexion()
        if self._cursor_ is not None:
            cursor = self._cursor_
        else:
            cursor = self.get_cursor()
        consider_params = None
        s = None
        concat_s = ""
        min_line = 0
        try:
            for s in script:
                s, consider_params, _type, nb_var = self._prepare_query(s, params, ignore_error)

                assert len(consider_params or []) <= self.MAX_PARAMETERS, "Max parameters reach. " \
                                                                          "Please consider this error."
                cursor = self._execute(cursor, s, params=consider_params,
                                       ignore_error=False,
                                       connexion=self.db_object)
                min_line += s.count("\n")
                if _type == 0:
                    if isinstance(params, (list, tuple, dict)):
                        params = list(params)
                    else:
                        params = []
                    for _ in range(nb_var):
                        params.pop(0)
        except Exception as ex:
            if isinstance(consider_params, (tuple, list)) and len(consider_params) > 20:
                consider_params = repr(consider_params[:20])[:-1] + f", ...] ({len(consider_params)})"
            else:
                consider_params = repr(consider_params)
            self._set_last_exception_after_execute(s, ex.args[0], min_line=min_line, params=consider_params)
            if concat_s:
                self.LAST_SQL_CODE_RUN = concat_s
            self.rollback()
            if not ignore_error:
                raise Exception(ex)
            else:
                self._print_info("*" * 10, "Got error when try to run", "*" * 10)
                self._print_info(s or self.LAST_SQL_CODE_RUN, consider_params)
                self._print_info("**" * 10)
                self._print_error(ex)
            return

        self.commit()
        if export:
            if export_name is None:
                export_name = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Downloads')
                if not os.path.exists(export_name):
                    export_name = os.getcwd()
                export_name = os.path.join(export_name, "export_data.csv")
                export_name = tools.get_no_filepath(export_name)

        if retrieve is None:
            retrieve = self._get_sql_type(script[-1]).lower() in ("with", "select")
        if retrieve:
            data = self.get_all_data_from_cursor(cursor, limit=limit, dict_res=dict_res,
                                                 export_name=export_name, sep=sep)
            if export_name is not None:
                return export_name
            if dict_res:
                if limit == 1:
                    return tools.Cdict(data)
                return [tools.Cdict(d) for d in data]
            return data

    def rollback(self):
        try:
            self.db_object.rollback()
        except (Exception, AttributeError):
            pass

    @staticmethod
    def get_add_increment_field_code(field_name="id"):
        return str(field_name or "id") + " INTEGER PRIMARY KEY AUTOINCREMENT"

    def create_table(self, arg: str | pandas.DataFrame | DatasetFactory, table_name=None, if_not_exists=True,
                     auto_increment_field=False,
                     auto_increment_field_name=None,
                     columns=None, ftype=None, verbose=True, only_structure=False, **kwargs):
        date_str_format = kwargs.get("date_str_format")
        if verbose:
            print = self._print_info
        else:
            print = lambda *args: None
        with self:
            if isinstance(arg, pandas.DataFrame):
                dataset = arg
            elif isinstance(arg, DatasetFactory):
                dataset = arg.dataset
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
        ftype = tools.Cdict(ftype)
        if columns is not None:
            if isinstance(columns, list):
                dataset = dataset.loc[:, columns]
            elif isinstance(columns, dict):
                dataset = dataset.loc[:, columns.keys()]
                dataset.rename(columns=columns, inplace=True)
        print("got %s data from file given" % dataset.shape[0])
        # print(dataset)
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

            if is_integer_dtype(dataset.iloc[:,index]) and (
                    is_integer_dtype(str(ftype.get(col)).lower()) or ftype.get(col) is None):
                table_script += f"\n\t{field} int"
            elif (is_float_dtype(dataset.iloc[:,index]) or is_integer_dtype(dataset.iloc[:,index])) and (
                    is_float_dtype(str(ftype.get(col)).lower()) or ftype.get(col) is None):
                table_script += f"\n\t{field} float"
            else:
                got = False
                if ftype.get(col) is not None:
                    got = True
                    table_script += f"\n\t{field} {ftype.get(col)}"

                if not got:
                    is_datetime_resp = _can_be_datetime_field(dataset.iloc[:,index], date_format=date_str_format)
                    if is_datetime_resp == "NO" and (
                            "date" not in str(ftype.get(col)).lower() or ftype.get(col) is None
                    ):
                        dataset.iloc[:,index] = dataset.iloc[:,index].apply(lambda x: str(x) if not pandas.isnull(x) else None)
                        max_ = max(dataset.iloc[:,index][:10000].apply(lambda x: len(str(x)) if x else 0))
                        min_ = min(dataset.iloc[:,index][:10000].apply(lambda x: len(str(x)) if x else 0))
                        if max_ == min_:
                            size = max_
                            if size == 0:
                                size = 255
                        else:
                            size = max_
                        if size > 255:
                            type_ = 'clob' if "oracle" in self._get_name.lower() else "text"
                        else:
                            if size > 3:
                                size = max(255, size)
                            type_ = f"varchar({size or 255})"
                    elif is_datetime_resp == "DATETIME":
                        type_ = "TIMESTAMP" if "oracle" in self._get_name.lower() else "datetime"
                        dataset.iloc[:,index] = dataset.iloc[:,index].apply(
                            lambda val: tools.CustomDateTime(str(val), default=None, ignore_errors=True)())
                    else:
                        dataset.iloc[:,index] = dataset.iloc[:,index].apply(
                            lambda val: tools.CustomDateTime(str(val), default=None, ignore_errors=True).date)
                        type_ = "date"

                    table_script += f"\n\t{field} {type_}"
            dataset.rename(columns={col: field}, inplace=True)

        table_script += "\n)"
        if if_not_exists:
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
        if only_structure:
            return
        self.insert_many(dataset, table_name=table_name, loader=kwargs.get("loader"))

    def dump(self, dump_file='dump.sql'):
        pass


def for_csv(row, sep=";"):
    # don't work well, use csv.write
    sep = str(sep or ",")
    rows = []
    for cell in row:
        if pandas.isnull(cell):
            cell = ""
        else:
            cell = str(cell)
            if sep in cell or "\n" in cell:
                # cell = re.sub(r'(?<!")"(?!")', '""', cell, flags=re.I)
                cell = cell.replace('"', '""')
                cell = f'"{cell}"'
        rows.append(cell)

    return sep.join(rows)
