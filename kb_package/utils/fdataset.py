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
import time
import typing
import inspect
import csv

import chardet
import numpy
import pandas

from pandas.core.dtypes.common import is_numeric_dtype, is_object_dtype
import kb_package.tools as tools
import kb_package.utils._query_func as query_func

from kb_package.logger import CustomLogger

Logger = CustomLogger("DatasetFactory")


def _parse_default_col_name(item, _preserve, _ignore_case, _columns):
    if not isinstance(item, str):
        return item
    if not _preserve:
        _item = tools.format_var_name(item)
        if _ignore_case:
            _item = _item.upper()
        if _item in _columns:
            return _item
    return item


def _preserve_or_transform_columns(dataset, preserve, ignore_case):
    if not preserve:
        renamed_col = {}
        warning_msg = []
        for ii, col in enumerate(dataset.columns):
            if isinstance(col, str):
                col_ = tools.format_var_name(col, accent=False)
                if not col:
                    col_ = "Columns_" + str(ii)
                if ignore_case:
                    col_ = col_.upper()
                renamed_col[col] = col_
                if col.upper() != col_:
                    warning_msg.append(repr(col) + " -> " + col_)
        if warning_msg:
            Logger.warning("Going to rename columns:", ",".join(warning_msg))

        dataset.rename(columns=renamed_col, inplace=True)
    return dataset


class DatasetFactory:
    LAST_FILE_LOADING_TIME = 0
    IGNORE_CASE = True

    def __init__(self, dataset: typing.Union[pandas.DataFrame, str, list, dict] = None, ignore_case=None,
                 preserve=False, **kwargs):
        # cls = self.__class__
        # cls.from_file.__code__.co_varnames
        self._path = None
        self._preserve = preserve
        self._ignore_case = self.IGNORE_CASE if ignore_case is None else ignore_case
        if isinstance(dataset, str):
            self._path = dataset
        if dataset is None:
            self._source = pandas.DataFrame()
        elif not isinstance(dataset, pandas.DataFrame) or (hasattr(dataset, "readable") and dataset.readable()):
            self._source = self.from_file(dataset, **kwargs, ignore_case=self._ignore_case, preserve=preserve).dataset
        else:
            self._source = _preserve_or_transform_columns(dataset, preserve=preserve,
                                                          ignore_case=ignore_case)

    def __parse_default_col_name(self, item):
        return _parse_default_col_name(item, _preserve=self._preserve,
                                       _ignore_case=self._ignore_case,
                                       _columns=self._source.columns)

    def __getattr__(self, item, default=None):
        item = self.__parse_default_col_name(item)
        # maybe methods or accessible attribute like columns
        try:
            return getattr(self.dataset, item)
        except AttributeError as ex:
            if default:
                return default
            raise ex

    def __setitem__(self, key, value):
        key = self.__parse_default_col_name(key)
        if self._ignore_case and not self._preserve and isinstance(key, str):
            key = key.upper()
        self.dataset.__setitem__(key, value)

    def __getitem__(self, item):
        item = self.__parse_default_col_name(item)
        return self.dataset.__getitem__(item)

    def __len__(self):
        return self._source.shape[0]

    def __delitem__(self, key):
        key = self.__parse_default_col_name(key)
        self._source.__delitem__(key)

    def save(self, path=None, force=False, **kwargs):
        path = path or self._path
        if path is None:
            raise TypeError("save method required :param path argument")
        if "index" not in kwargs:
            kwargs["index"] = False
        _base, ext = os.path.splitext(path)
        if force:
            i = 1
            while os.path.exists(path):
                path = _base + "_" + str(i) + ext
                i += 1
        if ext.lower() in [".xls", ".xlsx", ".xlsb"]:
            self._source.to_excel(path, **kwargs)
        elif ext.lower() in [".csv", ".txt", ""]:
            self._source.to_csv(path, **kwargs)
        return path

    @property
    def dataset(self):
        return self._source

    @staticmethod
    def _format_other(other, logic, **kwargs):
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
        elif isinstance(other, DatasetFactory):
            other = other.dataset
        other = DatasetFactory(other, **kwargs).dataset
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
        kwargs = {"ignore_case": self._ignore_case, "preserve": self._preserve}
        other, source_ref, other_ref = self._format_other(other, exclusion_logic, **kwargs)
        dataset = self.dataset.copy(deep=True)
        if isinstance(source_ref, str):
            source_ref = _parse_default_col_name(source_ref, _preserve=self._preserve,
                                                 _ignore_case=self._ignore_case,
                                                 _columns=self._source.columns)
        else:
            Logger.warning("use DataFrame merge method simply")
        if isinstance(other_ref, str):
            other_ref = _parse_default_col_name(source_ref, _preserve=self._preserve,
                                                _ignore_case=self._ignore_case,
                                                _columns=other.columns)
        else:
            Logger.warning("use DataFrame merge method simply")

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

    @staticmethod
    def _check_delimiter(sample, check_in=None):
        if check_in is None:
            check_in = [',', '\t', ';', ' ', ':']
        first_lines = "".join(sample)
        try:
            sep = csv.Sniffer().sniff(first_lines[:-1], delimiters=check_in).delimiter
        except csv.Error:
            sep = None
        return sep

    @classmethod
    def from_file(cls, file_path, sep=None, drop_duplicates=False, drop_duplicates_on=None, columns=None,
                  force_encoding=True, **kwargs):

        start_time = time.time()
        delimiters = kwargs.pop("delimiters", [',', '\t', ';', ' ', ':'])
        ignore_case = kwargs.pop("ignore_case", False)
        preserve = kwargs.pop("preserve", True)
        if "header" in kwargs and isinstance(kwargs["header"], bool):
            kwargs["header"] = None if not kwargs["header"] else "infer"

        if isinstance(file_path, str):
            try:
                is_hidden = bool(os.stat(file_path).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN)
            except AttributeError:
                # on Linux
                is_hidden = False
            if is_hidden:
                dataset = pandas.DataFrame()
            elif os.path.splitext(file_path)[1][1:].lower() in ["xls", "xlsx", "xlsm", "xlsb"]:
                kwargs_ = {
                    k: v for k, v in kwargs.items()
                    if k in pandas.read_excel.__wrapped__.__code__.co_varnames
                }
                dataset = pandas.read_excel(file_path, **kwargs_)
            else:
                kwargs_ = {
                    k: v for k, v in kwargs.items()
                    if k in pandas.read_csv.__wrapped__.__code__.co_varnames
                }
                if "encoding" not in kwargs:
                    kwargs_["encoding"] = "utf-8"
                used_sniffer = False
                try:
                    if sep is None:
                        with open(file_path, encoding=kwargs_["encoding"]) as file:
                            sample = [file.readline() for _ in range(10)]
                            try:
                                sep = DatasetFactory._check_delimiter(sample, delimiters)
                                assert sep, ""
                                kwargs_["sep"] = sep
                                used_sniffer = True
                            except (csv.Error, AssertionError):
                                pass
                    else:
                        kwargs_["sep"] = sep
                    dataset = pandas.read_csv(file_path, **kwargs_)
                except UnicodeDecodeError as exc:
                    if not force_encoding:
                        raise exc
                    with open(file_path, "rb") as file_from_file_path:
                        file_bytes = file_from_file_path.read()
                        encoding_proba = chardet.detect(file_bytes).get("encoding", "latin1")
                    if kwargs_["encoding"] != encoding_proba:
                        Logger.warning("We force encoding to:", encoding_proba)
                        kwargs_["encoding"] = encoding_proba
                        if "sep" not in kwargs_ or used_sniffer:
                            with open(file_path, encoding=kwargs_["encoding"]) as file:
                                sample = [file.readline() for _ in range(10)]
                                try:
                                    sep = DatasetFactory._check_delimiter(sample, delimiters)
                                    assert sep, ""
                                    kwargs_["sep"] = sep
                                except (csv.Error, AssertionError):
                                    pass
                        dataset = pandas.read_csv(file_path, **kwargs_)
                    else:
                        raise exc
        elif hasattr(file_path, "readable") and file_path.readable():
            sample = [file_path.readline() for _ in range(10)]
            file_path.seek(0)
            kk = {}
            if sep is None:
                try:
                    sep = DatasetFactory._check_delimiter(sample, delimiters)
                    assert sep, ""
                    kk["delimiter"] = sep
                except (csv.Error, AssertionError):
                    pass
            else:
                kk["delimiter"] = sep
            dataset = csv.DictReader(file_path, **kk)
        else:
            col_arg = columns
            if isinstance(columns, list):
                columns = None

            dataset = pandas.DataFrame(file_path, columns=col_arg,
                                       **{k: v for k, v in kwargs.items() if k in ["index", "dtype"]})

        cls.LAST_FILE_LOADING_TIME = time.time() - start_time
        dataset = _preserve_or_transform_columns(dataset, preserve=preserve,
                                                 ignore_case=ignore_case)

        if tools.BasicTypes.is_iterable(columns):
            final_col = {}
            dataset_columns = list(dataset.columns)
            for ii, k in enumerate(columns):
                alias = None
                if isinstance(k, dict):
                    k, alias = next(iter(k.items()))
                if k in dataset_columns:
                    key = k
                    alias = columns[k] if isinstance(columns, dict) else alias or k
                elif isinstance(k, str) and ii < len(dataset_columns):
                    key = dataset_columns[ii]
                    alias = columns[k] if isinstance(columns, dict) else alias or k
                elif isinstance(k, int):
                    key = dataset_columns[k]
                    alias = columns[k] if isinstance(columns, dict) else alias or key
                else:
                    raise ValueError("Bad column %s given" % k)
                final_col[key] = alias
            dataset = dataset.loc[:, final_col.keys()]
            dataset.rename(columns=final_col, inplace=True)

        if drop_duplicates or kwargs.get("rd") or kwargs.get("dd"):
            # rd -> remove duplicated, dd -> delete duplicated
            if drop_duplicates_on is None:
                drop_duplicates_on = kwargs.get("r_on") or kwargs.get("d_on")
            if drop_duplicates_on is None:
                drop_duplicates_on = dataset.columns
            else:
                drop_duplicates_on = ([drop_duplicates_on]
                                      if isinstance(drop_duplicates_on, str)
                                      else drop_duplicates_on)
                drop_duplicates_on = [_parse_default_col_name(d, _preserve=preserve,
                                                              _ignore_case=ignore_case,
                                                              _columns=dataset.columns)
                                      for d in drop_duplicates_on]
                drop_duplicates_on = dataset.columns.intersection(drop_duplicates_on)
            if drop_duplicates_on.shape[0]:
                dataset.drop_duplicates(inplace=True,
                                        subset=drop_duplicates_on, ignore_index=True)
        return cls(dataset)

    def doublon(self, drop=False):
        pass

    def __add__(self, other):
        if isinstance(other, self.__class__):
            other = other.dataset
        if self._source.empty or (
                len(self._source.columns) == len(other.columns) and all(self._source.columns == other.columns)):

            return DatasetFactory(pandas.concat([self._source, other], ignore_index=True, sort=False),
                                  preserve=self._preserve,
                                  ignore_case=self._ignore_case)
        return self._source.__add__(other)

    __radd__ = __add__

    def query(self, query, *, method="parse", **kwargs):
        """

        """
        query = query.strip()
        if not len(query) or self._source.empty:
            return self._source
        if method in ("parse", 1):
            permit_funcs = []
            params = QueryTransformer.PERMIT_FUNC
            for k, v in kwargs.items():
                if callable(v):
                    permit_funcs.extend(str(k).lower())
                    params[str(k).lower()] = v
            hard, query = QueryTransformer(permit_funcs=permit_funcs, ignore_case=self._ignore_case
                                           ).process(query)
            if hard:
                res = tools.safe_eval_math(query, params=params, dataset=self._source, method="exec", **kwargs)
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
        permit_funcs = ["pnn_ci"]
        params = QueryTransformer.PERMIT_FUNC
        pnn_ci = numpy.vectorize(lambda f, plus="+", reseaux="ORANGE", permit_fix=False: tools.BasicTypes.pnn_ci(
            f, plus, permit_fixe=permit_fix, reseau=reseaux))
        params["pnn_ci"] = pnn_ci
        for k, v in kwargs.items():
            if callable(v):
                permit_funcs.extend(str(k).lower())
                params[str(k).lower()] = v

        query = (
            "\ndef apply(serie):\n"
            "\tres = "
        )
        query += QueryTransformer("serie", hard=True, permit_funcs=permit_funcs,
                                  ignore_case=self._ignore_case).process(func,
                                                                         _for="apply")
        query += (
            "\n"
            "\treturn "
            "res.item() if hasattr(res, 'ndim') and "
            "res.ndim==0 else res\n"
            "result = dataset.apply(apply, axis=1) \n"
        )

        Logger.info("Going to run", query)

        return tools.safe_eval_math(query, params=params, dataset=self._source, method="exec", **kwargs)

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
            lambda m: (hasattr(m, "__module__") and callable(m) and (
                    m.__module__ == query_func.__name__ or
                    isinstance(m, numpy.vectorize))))
    }

    def __init__(self, *args, hard=False, permit_funcs=None, ignore_case=True):
        super().__init__()
        if len(args):
            self.PREFIX = args[0]
            if not self.PREFIX.endswith("."):
                self.PREFIX += "."
        self._custom_permit_func = permit_funcs or []
        for i in range(len(self._custom_permit_func)):
            self._custom_permit_func[i] = str(self._custom_permit_func[i]).lower()
        self._custom_permit_func.extend(list(self.PERMIT_FUNC.keys()))
        self._hard = hard
        self._use_attr = False
        self._ignore_case = ignore_case
        self.list_name = []

    def visit_Attribute(self, node):
        self._use_attr = True

    def visit_Name(self, node):
        # print("name", ast.dump(node, indent=2))
        if node.id.lower() not in ("null", "none"):
            self.list_name.append(node)
            if self._ignore_case:
                self._hard = True
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
        if node.func.id.lower() in self._custom_permit_func:
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
            query = query.replace('\n', " ")
            # replace = by ==
            query = re.sub(r"(?<![=><!])=(?![=])", "==", query, flags=re.I)
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
                    if self._ignore_case:
                        name.id = name.id.upper()
                    name.id = self.PREFIX + name.id
            res = ast.unparse(tree)
            if _for != "query":
                return res
            pycode = ""
            # with open(os.path.join(os.path.dirname(__file__), "_query_func.py")) as py:
            #    pycode = py.read()
            final_query = "result = " + self.PREFIX[:-1] + "[" + res + "]"
            pycode += final_query
            if verbose:
                Logger.info("Got final script -->", self.PREFIX[:-1] + "[" + res + "]")
            # code = compile(tree, "<string>", "exec")
            # exec(code)
        else:
            # here we consider the query is not hard so returns it
            if verbose:
                Logger.info("Got final script -->", old_query)
            pycode = old_query
        if _for != "query":
            return old_query

        return _hard, pycode


if __name__ == '__main__':
    p = DatasetFactory(r"C:\Users\FBYZ6263\Documents\OWN\kb_package\temp_test.csv")
    apply = "TYPE_CLIENT_ENDPERIOD + TYPE_CLIENT_ENDPERIOD if length(MSISDN)= 13 else 0"
    apply = "length(MSISDN)"
    print(p.apply(apply))
