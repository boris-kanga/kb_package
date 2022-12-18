# -*- coding: utf-8 -*-
"""
DatasetFactory object logic
Use like pandas.DataFrame extension.

"""
from __future__ import annotations
import ast

import os
import re
import stat
import typing
import inspect

import numpy
import pandas
from pandas.core.dtypes.common import is_numeric_dtype, is_object_dtype
import kb_package.tools as tools
import kb_package.utils._query_func as query_func


class DatasetFactory:
    def __init__(self, dataset: typing.Union[pandas.DataFrame, str, list, dict] = None, **kwargs):
        # cls = self.__class__
        # cls.from_file.__code__.co_varnames
        if dataset is None:
            self._source = pandas.DataFrame()
        elif not isinstance(dataset, pandas.DataFrame):
            self._source = self.from_file(dataset, **kwargs).dataset
        else:
            self._source = dataset

    def __getattr__(self, item, default=None):
        try:
            return getattr(self.dataset, item)
        except AttributeError as ex:
            if default:
                return default
            raise ex

    def __setitem__(self, key, value):
        self.dataset.__setitem__(key, value)

    def __getitem__(self, item):
        return self.dataset.__getitem__(item)

    def __len__(self):
        return self._source.shape[0]

    @property
    def dataset(self):
        return self._source

    @staticmethod
    def _format_other(other, logic):
        if isinstance(logic, dict):
            source_ref = logic["source"]
            other_ref = logic["exclude"]
        elif isinstance(logic, (list, tuple)):
            source_ref = logic[0]
            other_ref = (list(logic) + [source_ref])[1]
        elif isinstance(logic, str):
            source_ref = logic
            other_ref = logic
        else:
            raise ValueError(f"Bad value of logic given: {logic}")

        if isinstance(other, (list, pandas.Series)):
            other = pandas.DataFrame({other_ref: other})
        elif isinstance(other, str):
            other = DatasetFactory.from_file(other).dataset
        elif isinstance(other, DatasetFactory):
            other = other.dataset
        if not isinstance(other, pandas.DataFrame):
            raise ValueError("Bad value of other given: %s" % other)
        return other, source_ref, other_ref

    def __str__(self):
        return self.dataset.__str__() + " -> <kb_package | DatasetFactory>"

    def __repr__(self):
        return self.dataset.__repr__() + " -> <kb_package | DatasetFactory>"

    def c_merge(self,
                other: typing.Union[list, pandas.Series, pandas.DataFrame, str],
                exclusion_logic: typing.Union[dict, list, tuple, str],
                op="both", columns=None, suffixes=None):

        other, source_ref, other_ref = self._format_other(other, exclusion_logic)
        dataset = self.dataset.copy(deep=True)
        if is_numeric_dtype(dataset[source_ref]) and is_object_dtype(other[other_ref]):
            dataset[source_ref] = dataset[source_ref].apply(str)
        elif is_numeric_dtype(other[other_ref]) and is_object_dtype(dataset[source_ref]):
            other[other_ref] = other[other_ref].apply(str)
        result = dataset.merge(other,
                               left_on=source_ref,
                               right_on=other_ref,
                               how="left",
                               indicator=True,
                               suffixes=suffixes or ("", "_y"))
        if columns is None:
            columns = result.columns
        result = result.loc[result._merge == op, columns]
        return result.reset_index(drop=True)

    def exclude(self,
                other: typing.Union[list, pandas.Series, pandas.DataFrame, str],
                exclusion_logic: typing.Union[dict, list, tuple, str],
                ):
        return self.c_merge(other, exclusion_logic, op="left_only", columns=self.dataset.columns)

    def intersect(self,
                  other: typing.Union[list, pandas.Series, pandas.DataFrame, str],
                  exclusion_logic: typing.Union[dict, list, tuple, str],
                  ):
        return self.c_merge(other, exclusion_logic, op="both", columns=self.dataset.columns)

    @classmethod
    def from_file(cls, file_path, sep=None, drop_duplicates=False, drop_duplicates_on=None, columns=None, **kwargs):

        if isinstance(file_path, str):
            try:
                is_hidden = bool(os.stat(file_path).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN)
            except AttributeError:
                # on Linux
                is_hidden = False
            if is_hidden:
                dataset = pandas.DataFrame()
            elif os.path.splitext(file_path)[1][1:].lower() in ["xls", "xlsx", "xlsm", "xlsb"]:
                kwargs = {
                    k: v for k, v in kwargs.items()
                    if k in pandas.read_excel.__wrapped__.__code__.co_varnames
                }
                dataset = pandas.read_excel(file_path, **kwargs)
            else:
                kwargs = {
                    k: v for k, v in kwargs.items()
                    if k in pandas.read_csv.__wrapped__.__code__.co_varnames
                }
                if "encoding" not in kwargs:
                    kwargs["encoding"] = "utf-8"
                if sep is None:
                    with open(file_path, encoding='latin1') as file:
                        got = True
                        for _ in range(5):
                            line = file.readline().strip()
                            if line and ";" not in line:
                                got = False
                                break
                        if got:
                            kwargs["sep"] = ";"
                else:
                    kwargs["sep"] = sep
                try:
                    dataset = pandas.read_csv(file_path, **kwargs)
                except UnicodeDecodeError as exc:
                    if kwargs["encoding"] != "latin1":
                        print("We force encoding to latin1")
                        kwargs["encoding"] = "latin1"
                        dataset = pandas.read_csv(file_path, **kwargs)
                    else:
                        raise exc
        else:
            dataset = pandas.DataFrame(file_path, **{k: v for k, v in kwargs.items()
                                                     if k in ["index", "dtype"]})
        if columns is not None:
            if isinstance(columns, list):
                dataset = dataset.loc[:, columns]
            elif isinstance(columns, dict):
                dataset = dataset.loc[:, columns.keys()]
                dataset.rename(columns=columns, inplace=True)
        if drop_duplicates:
            drop_duplicates_on = ([drop_duplicates_on]
                                  if isinstance(drop_duplicates_on, str)
                                  else drop_duplicates_on)
            drop_duplicates_on = dataset.columns.intersection(drop_duplicates_on)
            if drop_duplicates_on.shape[0]:
                dataset.drop_duplicates(inplace=True,
                                        subset=drop_duplicates_on, ignore_index=True)
        return cls(dataset)

    def __add__(self, other):
        if self._source.empty or (
                len(self._source.columns) == len(other.columns) and all(self._source.columns == other.columns)):
            return DatasetFactory(pandas.concat([self._source, other], ignore_index=True, sort=False))
        return self._source.__add__(other)

    __radd__ = __add__

    def query(self, query, *, method="parse", **kwargs):
        """

        """
        query = query.strip()
        if not len(query) or self._source.empty:
            return self._source
        if method in ("parse", 1):
            hard, query = QueryTransformer().process(query)
            if hard:
                params = QueryTransformer.PERMIT_FUNC
                res = tools.safe_eval_math(query, params=params, dataset=self._source, method="exec")
                if kwargs.get("inplace"):
                    self._source = res
                    return
                return res
            else:
                return self._source.query(query, **kwargs)
        elif method in ("sql", 2):

            from kb_package.database.sqlitedb import SQLiteDB
            sql = SQLiteDB()
            sql.create_table(self.dataset)

            res = pandas.DataFrame(sql.run_script("select * from new_table where " + query, dict_res=True))
            if kwargs.get("inplace"):
                self._source = res
                return
            return res

    def apply(self, func, convert_dtype=True, *, args=(), **kwargs):
        if not isinstance(func, str):
            return self._source.apply(func, convert_dtype, args=args, **kwargs)
        query = "def apply(serie): return " + QueryTransformer("serie", hard=True).process(func, _for="apply") +"\n"
        query += "result = dataset.apply(apply, axis=1) \n"
        params = QueryTransformer.PERMIT_FUNC
        print("Going to run", query)
        params["pnn_ci"] = numpy.vectorize(lambda f, plus="+", reseaux="ORANGE", permit_fix=False: tools.BasicTypes.pnn_ci(
            f, plus, permit_fixe=permit_fix, reseau=reseaux))
        return tools.safe_eval_math(query, params=params, dataset=self._source, method="exec")

    def sampling(self, d: str | int):
        """
        return (statistically) representative sample
        """
        pass


class QueryTransformer(ast.NodeTransformer):
    PREFIX = "dataset."
    PERMIT_FUNC = {
        k: func
        for k, func in inspect.getmembers(
            query_func,
            lambda m: (callable(m) and (
                    m.__module__ == query_func.__name__ or
                    isinstance(m, numpy.vectorize))))
    }

    def __init__(self, *args, hard=False):
        super().__init__()
        if len(args):
            self.PREFIX = args[0]
            if not self.PREFIX.endswith("."):
                self.PREFIX += "."
        self._hard = hard
        self._use_attr = False
        self.list_name = []

    def visit_Attribute(self, node):
        self._use_attr = True

    def visit_Name(self, node):
        # print("name", ast.dump(node, indent=2))
        if node.id.lower() not in ("null", "none"):
            self.list_name.append(node)
        return node

    def visit_BoolOp(self, node):
        right = None
        for left in node.values[::-1]:
            left = self.visit(left)
            if right is None:
                right = left
                continue
            right = ast.BinOp(left, ast.BitAnd() if isinstance(node.op, ast.And) else ast.BitOr(), right)
        if right is None:
            right = node
        return ast.copy_location(right, node)

    def visit_UnaryOp(self, node):
        if not isinstance(node.op, ast.Not):
            return node
        node.op = ast.Invert()
        node.operand = self.visit(node.operand)
        return node

    def visit_Call(self, node):
        # print("call", ast.dump(node, indent=2))
        # node.func = self.visit(node.func)
        if node.func.id.lower() in QueryTransformer.PERMIT_FUNC:
            self._hard = True
            node.func.id = node.func.id.lower()
            args = []
            for d in node.args:
                d = self.visit(d)
                args.append(d)
            node.args = args
        else:
            raise ValueError("Got unknown function")
        return node

    def visit_Compare(self, node):
        node.left = self.visit(node.left)
        comparators = []
        for val in node.comparators:
            val = self.visit(val)
            comparators.append(val)
        node.comparators = comparators

        if isinstance(node.ops[0], (ast.Is, ast.IsNot)):
            if isinstance(node.comparators[0], ast.Tuple):
                # need to be 2 size -> (between _min and _max)
                self._hard = True
                _min, _max = node.comparators[0].elts

                result = ast.BoolOp(ast.And(), [
                    ast.Compare(node.left, [ast.GtE()], [_min]),
                    ast.Compare(node.left, [ast.LtE()], [_max])
                ])

                if isinstance(node.ops[0], ast.IsNot):
                    result = ast.UnaryOp(ast.Not(), result)
                result = self.visit(result)
                return ast.copy_location(result, node)
            elif isinstance(node.comparators[0], ast.Str):
                # field is "ksksjjsjsj" -> field like "djdjdj"
                self._hard = True
                result = ast.Call(ast.Name("like", ast.Load()),
                                  [node.left,
                                   node.comparators[0],
                                   ast.Constant(isinstance(node.ops[0], ast.IsNot))
                                   ], []
                                  )
                result = self.visit(result)
                return ast.copy_location(result, node)
            else:
                if node.comparators[0].id.lower() in ("null", "none"):
                    self._hard = True
                    result = ast.Compare(node.left,
                                         [ast.Eq() if isinstance(node.ops[0], ast.IsNot) else ast.NotEq()],
                                         [node.left])
                    result = self.visit(result)
                    return ast.copy_location(result, node)

        if isinstance(node.ops[0], (ast.In, ast.NotIn)):
            # print(ast.dump(node, indent=4))
            result = ast.Call(ast.Name("is_in", ast.Load()),
                              [node.left,
                               node.comparators[0],
                               ast.Constant(isinstance(node.ops[0], ast.NotIn))
                               ], []
                              )
            result = self.visit(result)
            return ast.copy_location(result, node)

        return node

    def process(self, node: str | ast.AST, verbose=False, _for="query"):
        if isinstance(node, str):
            query, quoted_text_dict = tools.replace_quoted_text(node)
            # replace = by ==
            query = re.sub(r"(?<!=)=(?!=)", "==", query, flags=re.I)
            # replace <> by !=
            query = query.replace("<>", "!=")

            # extra -> like and not like
            query = re.sub(r"\s+not\s+like\s+", " is not ", query, flags=re.I)
            query = re.sub(r"\s+like\s+", " is ", query, flags=re.I)
            # between
            query = re.sub(r"\s+(not\s+)?between\s+(\w+)\s+and\s+(\w+)", r" is \1 (\2, \3) ", query, flags=re.I)
            query = " " + query

            # keyword case
            for keyword in ["in", "is", "and", "or", "not"]:
                query = re.sub(r"\s+" + keyword + r"\s+", " " + keyword + " ", query, flags=re.I)

            query = query.strip()
            # consider the query where not hard
            old_query = query

            # for hard queries -- modifying of query
            # query = re.sub(r"\s+or\s+", " | ", query, flags=re.I)
            # query = re.sub(r"\s+and\s+", " & ", query, flags=re.I)

            for k in quoted_text_dict:
                query = query.replace(k, quoted_text_dict[k])
                old_query = old_query.replace(k, quoted_text_dict[k])
            node = ast.parse(query)
        else:
            old_query = ast.unparse(node)
        if not all([isinstance(n, ast.Expr) for n in node.body]):
            raise ValueError("Bad value given")
        tree = self.visit(node)
        #
        list_name = self.list_name
        _hard = self._hard
        #
        if _hard:
            if self._use_attr:
                raise NotImplementedError("Not permit to got attribute in this mode")
            for name in list_name:
                if not name.id.startswith(self.PREFIX):
                    name.id = self.PREFIX + name.id
            res = ast.unparse(tree)
            if _for != "query":
                return res
            pycode = ""
            # with open(os.path.join(os.path.dirname(__file__), "_query_func.py")) as py:
            #    pycode = py.read()
            final_query = "result = " + self.PREFIX[:-1] + "[" + res + "]"
            pycode += final_query
            if verbose or True:
                print("Got final script -->", self.PREFIX[:-1] + "[" + res + "]")
            # code = compile(tree, "<string>", "exec")
            # exec(code)
        else:
            # here we consider the query is not hard so returns it
            pycode = old_query
        if _for != "query":
            return old_query

        return _hard, pycode


if __name__ == '__main__':
    p = DatasetFactory(r"C:\Users\FBYZ6263\Documents\OWN\kb_package\temp_test.csv")
    print(p.query("upper(TYPE_CLIENT_ENDPERIOD) IN ('HYBRID', 'POSTPAID')"))
    apply = "TYPE_CLIENT_ENDPERIOD + TYPE_CLIENT_ENDPERIOD"
    print(p.apply(apply))
