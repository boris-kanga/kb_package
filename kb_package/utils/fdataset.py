# -*- coding: utf-8 -*-
"""
DatasetFactory object logic
Use like pandas.DataFrame extension.

"""
from __future__ import annotations

import os
import stat
import typing

import pandas
from pandas.core.dtypes.common import is_numeric_dtype, is_object_dtype


class DatasetFactory:
    def __init__(self, dataset: typing.Union[pandas.DataFrame, str, list, dict], **kwargs):
        # cls = self.__class__
        # cls.from_file.__code__.co_varnames
        self._source = self.from_file(dataset, **kwargs).dataset

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
            other_ref = (logic + [source_ref])[1]
        elif isinstance(logic, str):
            source_ref = logic
            other_ref = logic
        else:
            raise ValueError(f"Bad value of logic given: {logic}")

        if isinstance(other, (list, pandas.Series)):
            other = pandas.DataFrame({other_ref: other})
        elif isinstance(other, str):
            other = DatasetFactory.from_file(other).dataset
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

        result = result.loc[result._merge == op, columns or result.columns]
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

    def sql_query(self):
        """

        """
        pass

    def sampling(self, d: str | int):
        self._source.sample()


if __name__ == '__main__':
    p = DatasetFactory(
        r"C:\Users\FBYZ6263\Documents\WORK_FOLDER\CVM\Push SMS\Databases\Recrutement_New Community Plus_B2B.csv",
        sep=";")
    print(p.dataset)
