# -*- coding: utf-8 -*-
"""
The MongoDB manager.
Use for run easily MongoDB requests

Required pymongo~=3.12.0
"""
import re
import importlib
import traceback

import pymongo

from kb_package.database.where_clause import WhereClause
from kb_package.database.basedb import BaseDB
from kb_package.tools import INFINITE, Cdict
from kb_package import tools


class MongoDB(BaseDB):
    SQL_TYPE_ACCEPTED = ["SELECT", "INSERT", "DELETE", "UPDATE"]
    SQL_SELECT_REGEX = r"^SELECT\s+(?P<FIELDS>.*?)\s+" \
                       r"FROM\s+(?P<TABLE>.*?)\s+" \
                       r"(?:WHERE\s+(?P<WHERE_CLAUSE>.*?))?" \
                       r"(?:GROUP\s+BY\s+(?P<GROUP_BY>.*?))?" \
                       r"(?:HAVING\s+(?P<HAVING>.*?))?" \
                       r"(?:ORDER\s+BY\s+(?P<ORDER_BY>.*?))?" \
                       r"(?:LIMIT\s+(?P<LIMIT>.*?))?$"

    SQL_AGGREGATE_REGEX = r"^(AVG|SUM|COUNT|MAX|MIN)\((.*?)\)" \
                          r"(?:(?:\sas)?\s(\w+))?$"
    DEFAULT_PORT = 27017

    @property
    def _get_name(self):
        return self.__class__.__name__

    # implementation of abstracts methods
    @staticmethod
    @tools.many_try(max_try=1, sleep_time=0, error_manager_key="BD_MONGO")
    def connect(
            host="127.0.0.1", user="root", password="", db_name=None,
            port=DEFAULT_PORT, **kwargs
    ) -> pymongo.MongoClient:
        """
        Making the connexion to the mongo database
        Args:
            host: str, the Mysql server ip
            user: str, the username for mysql connexion
            password: str, the password
            db_name
            port: int port

        Returns: pymongo.MongoClient, the connexion object reach

        """
        try:

            db = pymongo.MongoClient(
                host=host,
                username=user,
                password=password,
                port=port
            )

            return db

        except Exception as ex:
            ex.args = ["Une erreur lors que la connexion à la base de donnée --> " + str(ex.args[0])] + \
                      list(ex.args[1:])
            raise ex

    def _cursor(self):
        return self.db_object[self.database_name]

    @staticmethod
    def _fetchone(cursor, limit=INFINITE):
        index_data = 0
        for d in list(cursor):
            if index_data >= limit:
                break
            yield d
            index_data += 1

    @staticmethod
    def _get_cursor_description(cursor):
        return Cdict(columns=[d for d in cursor[0]])

    @staticmethod
    @tools.many_try(max_try=1, sleep_time=0, error_manager_key="BD_MONGO")
    def _execute(cursor, script, params=None, ignore_error=False, **kwargs):
        """
        use to make preparing requests
        Args:
            cursor: mysql.connector.cursor.MySQLCursor
            script: object, the prepared requests
            params: list|tuple|dict, params for the mysql prepared requests

        Returns: mysql.connector.cursor.MySQLCursor, the cu

        """
        method = script.get("method")
        params = script.get("pipeline")
        if not isinstance(params, dict):
            params = {}

        try:
            cursor = getattr(cursor, method)(**params)
            return cursor
        except Exception as ex:
            if ignore_error:
                return None
            raise ex

    def create_table(self, arg: str, table_name=None, auto_increment_field=False, auto_increment_field_name=None,
                     **kwargs):
        pass

    @staticmethod
    def eval_sql_type(sql_code, regex):
        res = re.match(regex, sql_code,
                       flags=re.I | re.S)
        if res:
            return res.groupdict()
        return res

    def run_script_module(self, module, function_name, params=None):
        """

        Args:
            module: str, specify the module you want to run
            function_name: str, the name of request function
            params: object, the giving params

        Returns: data, the results of the requests
        """
        try:
            self.reload_connexion()
            module = importlib.import_module(module)
            return getattr(module, function_name)(self.db_object, params)
        except (Exception, ImportError):
            print(traceback.format_exc())

    @staticmethod
    def fields_parser(fields: str):
        fields = [f.strip() for f in fields.strip().split(",")]
        if "*" in fields:
            # think about -> example: select *, IIF(a, 2, 3) as t from ...
            return None
        final_fields = {"_id": 0}
        aggregate_fields = {}
        for k in fields:
            # think about using of function like iif, lower, replace, regex, <constant> ...
            res = re.match(MongoDB.SQL_AGGREGATE_REGEX, k, flags=re.I | re.S)
            if res:
                aggregate, var, alias = res.groups()
                var = var.strip()
                alias = (alias or k).strip()

                if var == "*":
                    var = 1
                else:
                    var = "$" + var

                aggregate = aggregate.lower()
                if aggregate == "count":
                    aggregate = "sum"
                    var = 1
                aggregate_fields[alias] = {"$" + aggregate: var}
            else:
                var, alias = re.match(r"^(\w+?)(?:(?:\sas)?\s(\w+))?$", k,
                                      flags=re.I | re.S).groups()
                if alias is not None:
                    var = {
                        alias.strip(): "$" + var
                    }
                else:
                    var = {
                        var.strip(): 1
                    }

                final_fields.update(var)

        return final_fields, aggregate_fields

    @staticmethod
    def execute_from_sql(sql_code: str, sql_type: str = None):
        sql_code = sql_code.strip()
        sql_code, quoted_text_dict = tools.replace_quoted_text(sql_code)
        res = None
        if sql_type is not None:
            sql_type = sql_type.upper()
            regex = getattr(MongoDB, "SQL_" + sql_type + "_REGEX")
            res = MongoDB.eval_sql_type(sql_code, regex)
        else:
            for regex in MongoDB.SQL_TYPE_ACCEPTED:
                res = MongoDB.eval_sql_type(
                    sql_code, getattr(MongoDB, "SQL_" + regex + "_REGEX"))
                if res:
                    sql_type = regex
                    break
        print(res)
        method = None
        args = []
        collection = None
        if res:
            # print(res)
            if sql_type == "SELECT":
                method = "aggregate"
                args.append([])
                obj = args[-1]
                for k in quoted_text_dict:
                    for kk in res:
                        res[kk] = res[kk].replace(k, quoted_text_dict[k])
                fields, collection, where_clause, group_by, order_by, limit = \
                    res.values()

                projection, aggregate_fields = MongoDB.fields_parser(fields)

                where_clause = WhereClause.parse_where_clause_to_mongo(
                    where_clause)
                if len(where_clause):
                    obj.append({"$match": where_clause})
                if group_by is not None or len(aggregate_fields):
                    if isinstance(group_by, str):
                        group_by = {f.strip(): "$" + f.strip()
                                    for f in group_by.split(",")}
                        for f in group_by:
                            if projection.get(f, 0):
                                projection[f] = "$_id." + f
                    else:
                        group_by = None
                    group_by = {"_id": group_by}
                    group_by.update(aggregate_fields)

                    obj.append({"$group": group_by})

                    for key in aggregate_fields.keys():
                        projection[key] = 1

                if isinstance(projection, dict) and len(projection):
                    obj.append({"$project": projection})

                if order_by:
                    # order_by, _ = MongoDB.fields_parser(order_by)
                    # obj.append({"$sort": where_clause})
                    pass
                print(f"db.{collection}.{method}({args[0]})")

        return {"collection": collection, "method": method, "args": args}


if __name__ == "__main__":
    print(MongoDB.execute_from_sql("""
    SELECT count(name), name FROM test
        WHERE value3>1 group by name"""))
