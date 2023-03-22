# -*- coding: utf-8 -*-
"""
DatasetFactory object logic
Use like pandas.DataFrame extension.

"""
from __future__ import annotations
from builtins import Ellipsis
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


# agg custom func
def count_distinct(s):
    return len(s.unique())


class DatasetFactory:
    LAST_FILE_LOADING_TIME = 0
    IGNORE_CASE = True

    def __init__(self, dataset: typing.Union[pandas.DataFrame, str, list, dict] = None, ignore_case=None,
                 preserve=False, dd=False, drop_on=None, **kwargs):
        # cls = self.__class__
        # cls.from_file.__code__.co_varnames
        self.__path = None
        self.__ignore_case = self.IGNORE_CASE if ignore_case is None else ignore_case
        if isinstance(dataset, str):
            self.__path = dataset
        if dataset is None:
            self.__source = pandas.DataFrame()
        elif not isinstance(dataset, pandas.DataFrame) or (hasattr(dataset, "readable") and dataset.readable()):
            self.__source = self.from_file(dataset, **kwargs, ignore_case=self.__ignore_case, preserve=preserve).dataset
        else:
            self.__source = dataset
        self.columns = pandas.Index([tools.Var(col) for col in self.__source.columns])
        self._preserve = preserve

        if dd or kwargs.get("drop_duplicates") or kwargs.get("rd"):
            if drop_on is None:
                drop_on = kwargs.get("r_on") or kwargs.get("d_on") or kwargs.get("drop_duplicates_on")
            self.doublon(drop_on=drop_on)

    # Ok
    @staticmethod
    def __parse_col(col, columns):
        for item in columns:
            if tools.Var(item) == col:
                return item
        return col

    # Ok
    def __parse_default_col_name(self, col):
        return self.__parse_col(col, self.columns)

    # Ok
    def doublon(self, drop_on=None, keep="first"):
        if drop_on is None:
            drop_on = self.columns
        else:
            drop_on = ([drop_on] if isinstance(drop_on, str) else drop_on)

            drop_on = [self.__parse_default_col_name(k) for k in drop_on]

            drop_on = self.columns.intersection(drop_on)
        if drop_on.shape[0]:
            self.drop_duplicates(inplace=True, keep=keep, subset=drop_on, ignore_index=True)

    # Ok
    def __getattr__(self, item, default=None):
        item = self.__parse_default_col_name(item)
        # maybe methods or accessible attribute like columns
        try:
            return getattr(self.__source, item)
        except AttributeError as ex:
            if default:
                return default
            raise ex

    # Ok
    def __setitem__(self, key, value):
        key = self.__parse_default_col_name(key)
        self.__source.__setitem__(key, value)
        self.columns = pandas.Index([tools.Var(col) for col in self.__source.columns])

    # Ok
    def __getitem__(self, item):
        item = self.__parse_default_col_name(item)
        return self.__source.__getitem__(item)

    # Ok
    def __len__(self):
        return self.__source.shape[0]

    # Ok
    def __delitem__(self, key):
        key = self.__parse_default_col_name(key)
        self.__source.__delitem__(key)
        self.columns = pandas.Index([tools.Var(col) for col in self.__source.columns])

    # Ok
    def __delattr__(self, item):
        key = self.__parse_default_col_name(item)
        self.__source.__delitem__(key)
        self.columns = pandas.Index([tools.Var(col) for col in self.__source.columns])

    # Ok
    def __setattr__(self, key, value):
        if (key in ["_DatasetFactory" + pp for pp in ["__source", "__path", "__ignore_case"]] or
                key in ("columns", "_preserve")):
            super().__setattr__(key, value)
            return
        key = self.__parse_default_col_name(key)
        setattr(self.__source, key, value)
        self.columns = pandas.Index([tools.Var(col) for col in self.__source.columns])

    # Ok
    def save(self, path=None, force=False, chdir=True, **kwargs):
        path = path or self.__path

        if path is None:
            raise TypeError("save method required :param path argument")
        if chdir:
            self.__path = path
        if "index" not in kwargs:
            kwargs["index"] = False
        _base, ext = os.path.splitext(path)
        if force:
            i = 1
            while os.path.exists(path):
                path = _base + "_" + str(i) + ext
                i += 1
        if ext.lower() in [".xls", ".xlsx", ".xlsb"]:
            self.__source.to_excel(path, **kwargs)
        elif ext.lower() in [".csv", ".txt", ""]:
            self.__source.to_csv(path, **kwargs)
        return path

    # Ok
    @property
    def dataset(self):
        return self.__source.rename(columns={k: self.columns[i] for i, k in enumerate(self.__source.columns)})

    # Ok
    def __str__(self):
        return self.dataset.__str__() + "\n<kb_package | DatasetFactory>"

    # Ok
    def __repr__(self):
        return self.dataset.__repr__() + "\n<kb_package | DatasetFactory>"

    # Ok
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
        elif isinstance(other, DatasetFactory):
            other = other.dataset
        other = DatasetFactory(other)
        return other, source_ref, other_ref

    # Ok
    def cmerge(self,
               other: typing.Union[list, pandas.Series, pandas.DataFrame, str],
               exclusion_logic: typing.Union[dict, list, tuple, str],
               op=None, columns=None, suffixes=None, how="left"):

        other, source_ref, other_ref = self._format_other(other, exclusion_logic)
        dataset = self.dataset.copy(deep=True)
        if isinstance(source_ref, str):
            source_ref = [source_ref]
        if isinstance(other_ref, str):
            other_ref = [other_ref]
        ref_size = min(len(source_ref), len(other_ref))
        for i in range(ref_size):
            s_ref = self.__parse_default_col_name(source_ref[i])
            o_ref = other.__parse_default_col_name(other_ref[i])
            other_ref[i] = o_ref
            source_ref[i] = s_ref
            if is_numeric_dtype(dataset[s_ref]) and is_object_dtype(other[o_ref]):
                dataset[s_ref] = dataset[s_ref].apply(str)
            elif is_numeric_dtype(other[o_ref]) and is_object_dtype(dataset[s_ref]):
                other[o_ref] = other[o_ref].apply(str)

        result = dataset.merge(other.dataset,
                               left_on=source_ref[:ref_size],
                               right_on=other_ref[:ref_size],
                               how=how,
                               indicator=True,
                               suffixes=suffixes or ("", "_y"))
        if columns is None:
            columns = result.columns
        if isinstance(op, str):
            result = result.loc[result._merge == op, columns]
        else:
            result = result.loc[:, columns]
        return result.reset_index(drop=True)

    # Ok
    def exclude(self,
                other: typing.Union[list, pandas.Series, pandas.DataFrame, str],
                exclusion_logic: typing.Union[dict, list, tuple, str],
                ):
        return self.cmerge(other, exclusion_logic, op="left_only", columns=self.dataset.columns)

    # Ok
    def intersect(self,
                  other: typing.Union[list, pandas.Series, pandas.DataFrame, str],
                  exclusion_logic: typing.Union[dict, list, tuple, str],
                  ):
        return self.cmerge(other, exclusion_logic, op="both", columns=self.dataset.columns)

    # Ok
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

    # Ok
    @classmethod
    def from_file(cls, file_path, sep=None, columns=None,
                  force_encoding=True, **kwargs):

        start_time = time.time()
        delimiters = kwargs.pop("delimiters", [',', '\t', ';', ' ', ':'])
        ignore_case = kwargs.pop("ignore_case", False)
        preserve = kwargs.pop("preserve", True)
        if "header" in kwargs and isinstance(kwargs["header"], bool):
            kwargs["header"] = None if not kwargs["header"] else "infer"
        if isinstance(file_path, cls):
            dataset = file_path.dataset
        elif isinstance(file_path, str):
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

        if tools.BasicTypes.is_iterable(columns):
            final_col = {}
            dataset_columns = [tools.Var(col) for col in dataset.columns]
            first = next(iter(columns))
            without = False
            if first is Ellipsis:
                without = True
                final_col = {k: k for k in dataset_columns}

            for k in columns:
                alias = None
                if k is Ellipsis:
                    continue
                if isinstance(k, dict):
                    k, alias = next(iter(k.items()))
                if k in dataset_columns:
                    key = cls.__parse_col(k, dataset_columns)
                    alias = columns[k] if isinstance(columns, dict) else alias or k
                elif isinstance(k, str):
                    # à supprimer
                    key = k
                    alias = columns[k] if isinstance(columns, dict) else alias or k
                elif isinstance(k, int):
                    key = dataset_columns[k]
                    alias = columns[k] if isinstance(columns, dict) else alias or key
                else:
                    raise ValueError("Bad column %s given" % k)
                if without:
                    final_col.pop(key)
                else:
                    final_col[key] = alias
            dataset = dataset.loc[:, final_col.keys()]
            dataset.rename(columns=final_col, inplace=True)

        return cls(dataset, preserve=preserve, ignore_case=ignore_case)

    @staticmethod
    def _gen_columns_by_string(dataframe, op, alias=None):
        """
        op like col1 + col2 --> retrieve the dataframe with added column col1 + col2
        """
        if op in [tools.Var(d) for d in dataframe.columns]:
            return dataframe
        dataframe[alias or tools.format_var_name(op, permit_char="+-*/")] = DatasetFactory(dataframe).apply(op)
        return dataframe

    # Ok
    def sql(self, query=None, *, select=None, group_by=None, where=None):
        """
        query: str like @select col1, col2 @where [condition] @group_by col1, col2
        """
        final_query = {}
        if query is not None:
            query, quoted_text_dict = tools.replace_quoted_text(query)
            query = re.split(r"@(select|where|group_by)\s+(?P<select>[^@]+)", query, flags=re.I | re.S)
            query = [query[i] for i in range(len(query)) if i % 3 != 0]

            for i in range(len(query)):
                if query[i].lower() in "select|where|group_by".split('|'):
                    v = query[i + 1].strip()
                    final_query[query[i].lower()] = v

        select = select or final_query.get("select")
        group_by = group_by or final_query.get("group_by")
        where = where or final_query.get("where")
        temp = self.dataset
        if where:
            where = where.strip()
            if where:
                temp = self.query(where)
        final_select = select
        if select:
            if isinstance(select, str):
                final_select = []
                for s in select.split(","):
                    s = s.strip()
                    res = re.search(r"(avg|count|min|max|sum)\((.*?)\)(?:\s+(?:as\s+)?([^,]+))?", s, flags=re.I)
                    if res:
                        res = res.groups()
                        col = self.__parse_default_col_name(res[1])
                        func = res[0].strip()
                        alias = res[2]
                        if col == "*" and func not in ("count", "size"):
                            raise ValueError("Bad value of select %s " % (s,))
                        elif col == "*":
                            func = "count"
                            col = temp.columns[0]
                        elif func in ("size", "count") and re.search(r"distinct\s+(\w+)", col, flags=re.I | re.S):
                            col = self.__parse_default_col_name(
                                re.search(r"distinct\s+(\w+)", col, flags=re.I | re.S).groups()[0])
                            func = count_distinct
                        final_select.append({"func": func, "on": col, "alias": alias})
                    else:
                        s = re.search(r"(\w+)(?:\s+as)?(?:\s+(.*))?", s, flags=re.I | re.S)
                        final_select.append({"on": s[0], "alias": s[1]})

        if group_by:
            temp = self.group(group_by, temp, [d for d in final_select if isinstance(d, dict) and d.get("func")])
        for d in final_select:
            if "func" not in d:
                temp = self._gen_columns_by_string(temp, d["on"], d.get("alias"))
        return DatasetFactory(temp)

    # Ok
    @staticmethod
    def group(group_by, dataset, aggregating_func=None):
        """
        aggregating_func like [{func: avg, on:field}]
        """
        if isinstance(group_by, str):
            group_by = [DatasetFactory.__parse_col(d, dataset.columns) for d in group_by.split(",") if d.strip()]

        group_by_elm = dataset.groupby(by=group_by)
        _equivalence = {"avg": "mean", "count": "size"}
        final_agg = {}
        for d in aggregating_func or []:
            func = _equivalence.get(d["func"]) or d["func"]
            # d["func"] = func
            alias = d.get("alias") or (d["func"] + f"({d['on']})" if isinstance(d["func"], str)
                                       else d["func"].__name__ + f"({d['on']})")
            final_agg[alias] = pandas.NamedAgg(column=d['on'], aggfunc=func)
        if final_agg:
            data = group_by_elm.agg(**final_agg)
            """
            aggregating_func = [tools.Cdict(d) for d in aggregating_func]
            data = tools.concurrent_execution(lambda by: getattr(group_by_elm[by.on], by.func)(),
                                              len(aggregating_func),
                                              args=lambda index: (aggregating_func[index], index))
            """
            final_d = {}
            index_rows = [d for d in data.index]
            if len(group_by) > 1:
                final_d.update({d: [row[c] for row in index_rows] for c, d in enumerate(group_by)})
            else:
                final_d[group_by[0]] = index_rows
            for col in final_agg:
                final_d[col] = data[col].values

            data = pandas.DataFrame(final_d)
        else:
            data = group_by_elm.size()
            index_rows = [d for d in data.index]

            if len(group_by) > 1:
                final_agg.update({d: [row[c] for row in index_rows] for c, d in enumerate(group_by)})
            else:
                final_agg[group_by[0]] = index_rows
            final_agg["COUNT"] = data.values
            data = pandas.DataFrame(final_agg)
        return data

    # Ok
    def __add__(self, other):
        if isinstance(other, self.__class__):
            other = other.dataset
        if self.__source.empty or (
                len(self.columns) == len(other.columns) and all(self.columns == other.columns)):
            return DatasetFactory(pandas.concat([self.__source, other], ignore_index=True, sort=False),
                                  preserve=self._preserve,
                                  ignore_case=self.__ignore_case)
        return self.__source.__add__(other)

    __radd__ = __add__

    # Ok
    def query(self, query, params=None, *, method="parse", **kwargs):
        """

        """
        query = query.strip()
        if not len(query) or self.__source.empty:
            return self.__source
        if method in ("parse", 1):
            permit_funcs = []
            q_permit_funcs = QueryTransformer.PERMIT_FUNC
            for k, v in kwargs.items():
                if callable(v):
                    permit_funcs.extend(str(k).lower())
                    q_permit_funcs[str(k).lower()] = v

            var_root_name = tools._get_new_kb_text(" ".join(self.__source.columns))

            eq_col = {col: tools.format_var_name(col, default=var_root_name + "_" + str(index))
                      for index, col in enumerate(self.__source.columns)}
            self.__source.rename(columns=eq_col, inplace=True)

            hard, query, concerned_names = QueryTransformer(permit_funcs=permit_funcs).process(
                query,
                params=params,
                columns=list(self.__source.columns))
            # dataset.rename(columns={self.__parse_default_col_name(col): col for col in concerned_names}, inplace=True)
            inplace = kwargs.pop("inplace", False)
            if hard:
                res = tools.safe_eval_math(query, params=q_permit_funcs, dataset=self.__source, method="exec", **kwargs)
            else:
                res = self.__source.query(query, **kwargs)
            res = res.rename(columns={v: k for k, v in eq_col.items()})
            if inplace:
                self.__source = res
                return
            self.__source.rename(columns={v: k for k, v in eq_col.items()}, inplace=True)
            return res
        elif method in ("sql", 2):

            from kb_package.database.sqlitedb import SQLiteDB
            sql = SQLiteDB()
            sql.create_table(self.dataset)

            res = pandas.DataFrame(sql.run_script("select * from new_table where " + query, dict_res=True))
            if kwargs.get("inplace"):
                self.__source = res
                return
            return res

    # Ok
    def apply(self, func, convert_dtype=True, params=None, *, args=(), **kwargs):
        if not isinstance(func, str):
            return self.__source.apply(func, convert_dtype, args=args, **kwargs)
        permit_funcs = ["pnn_ci"]
        q_permit_funcs = QueryTransformer.PERMIT_FUNC
        pnn_ci = numpy.vectorize(lambda f, plus="+", reseaux="ORANGE", permit_fix=False: tools.BasicTypes.pnn_ci(
            f, plus, permit_fixe=permit_fix, reseau=reseaux))
        q_permit_funcs["pnn_ci"] = pnn_ci
        for k, v in kwargs.items():
            if callable(v):
                permit_funcs.extend(str(k).lower())
                q_permit_funcs[str(k).lower()] = v

        var_root_name = tools._get_new_kb_text(" ".join(self.__source.columns))

        eq_col = {col: tools.format_var_name(col, default=var_root_name + "_" + str(index))
                  for index, col in enumerate(self.__source.columns)}
        self.__source.rename(columns=eq_col, inplace=True)

        res = QueryTransformer("serie", hard=True, permit_funcs=permit_funcs).process(
            func,
            _for="apply",
            params=params)
        query, concerned_names = res
        query = (
                    "\ndef apply(serie):\n"
                    "\tres = "
                ) + query
        query += (
            "\n"
            "\treturn "
            "res.item() if hasattr(res, 'ndim') and "
            "res.ndim==0 else res\n"
            "result = dataset.apply(apply, axis=1) \n"
        )

        Logger.info("Going to run", query)
        res = tools.safe_eval_math(query, params=q_permit_funcs, dataset=self.__source, method="exec", **kwargs)
        self.__source.rename(columns={v: k for k, v in eq_col.items()}, inplace=True)
        return res

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

    def process(self, node: str | ast.AST, verbose=False, _for="query", params=None, columns=None):
        if isinstance(node, str):
            if columns is None:
                columns = []
            if isinstance(params, dict):
                params = {k: repr(v) for k, v in params.items()}
            elif isinstance(params, (list, tuple)):
                params = tuple([repr(v) for v in params])
            else:
                params = None
            if params is not None:
                try:
                    node = node % params
                except (TypeError, Exception):
                    raise ValueError("Bad value of params: %s  -- for the query: %s" % (params, repr(node)))

            query, quoted_text_dict = tools.replace_quoted_text(node)
            query = query.replace('\n', " ")

            # check for variable @1 -> col1 of the dataset
            def eq_col(match):
                index = int(match.groups()[0][1:]) - 1

                return columns[index]

            query = re.sub(r"(@\d+)", eq_col, query)
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
        concerned_names = [name.id for name in list_name]
        #
        if _hard:
            if self._use_attr:
                raise NotImplementedError("Not permit to got attribute in this mode")
            for name in list_name:
                if not name.id.startswith(self.PREFIX):
                    name.id = self.PREFIX + name.id

            res = ast.unparse(tree)
            if _for != "query":
                return res, concerned_names
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
            return old_query, concerned_names

        return _hard, pycode, concerned_names


def __test__():
    columns = ["", "Col test", "kb_vars"]
    temp = {"": range(100), "Col test": ["uuu" + str(x) for x in range(100)], "kb_vars": "test"}
    temp = DatasetFactory(temp)
    assert temp.query("(@1>10 and @2 like %s) or @1 in %s", params=(r"\w+9\d+", [2, 3, 4])).shape[0] == 13, \
        "Error with the method query"
    assert all([col == col_t for col, col_t in zip(temp.columns, columns)])
    assert temp["col test"].shape[0] == 100


if __name__ == '__main__':
    def ttt(x, *args):
        return "|".join(x)


    d = DatasetFactory(r"C:\Users\FBYZ6263\Downloads\Pass KDO avec Avec zone.csv")
    print(d.sql(group_by=["DEPARTEMENT", "COMMUNE"]))
    d.sql(group_by=["DEPARTEMENT", "COMMUNE"]).save("base_pass_kdo_par_zone.csv")
