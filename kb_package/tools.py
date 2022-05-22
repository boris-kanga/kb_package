# -*- coding: utf-8 -*-
"""
All generals customs tools we develop.
they can be use in all the project
"""

import datetime
import json
import logging
import os
import re
import sys
import time
import tarfile
import traceback
import zipfile
from typing import Union


def _init_infinite(self, s=1):
    self._s = s > 0


def add_query_string_to_url(url, params):
    from requests.models import PreparedRequest
    try:

        req = PreparedRequest()
        req.prepare_url(url, params)
        return req.url
    except:
        traceback.print_exc()
    return ""


def safe_eval_math(calculation, params=None):
    import numexpr
    if not isinstance(params, dict):
        params = {}
    for k, v in params.items():
        locals()[k] = v
    res = ""
    try:
        res = numexpr.evaluate(calculation)
        return list(res)
    except TypeError:
        return res.item()
    except:
        return ""


_infinite_methods = {
    "__init__": _init_infinite,
    "__str__": lambda self: ("+" if self > 0 else "-") + "Inf",
    "__repr__": lambda self: ("+" if self > 0 else "-") + "Inf",
    "__gt__": lambda self, _: self._s > 0,
    "__pow__": lambda self, _: self._s > 0,
    "__ge__": lambda self, _: self._s > 0,
    "__lt__": lambda self, _: self._s < 0,
    "__le__": lambda self, _: self._s < 0,
    "__neg__": lambda self: self.__class__(-self._s),
    "__rtruediv__": lambda self, _: 0,
    "__ne__": lambda self, _: True,
    "__eq__": lambda self, _: False,
}
_infinite_methods.update(
    {
        "__" + key + "__": (lambda self, _: self)
        for key in [
        "add",
        "radd",
        "sub",
        "rsub",
        "pow",
        "rpow",
        "round",
        "ceil",
        "abs",
        "floor",
        "mul",
        "rmul",
        "truediv",
    ]
    }
)

INFINITE = type("Infinite", (float,), _infinite_methods)()


def extract_file(path, member=None, to_directory='.', file_type=None):
    members = [None]
    if isinstance(member, str):
        members = [member]
    elif isinstance(member, list):
        members = member
    method = "extractall"
    if not isinstance(file_type, str):
        _, file_type = os.path.splitext(path)
    file_type = str(file_type).lower().strip()
    if file_type.startswith("."):
        file_type = file_type[1:]
    assert file_type in ["zip", "tgz", "tar.gz", "tar.bz2", "tbz"], (
            "Bad file %s given" % path)
    if member is not None:
        method = "extract"
    if file_type == 'zip':
        opener, mode = zipfile.ZipFile, 'r'
    elif file_type == 'tar.gz' or file_type == 'tgz':
        opener, mode = tarfile.open, 'r:gz'
    elif file_type == 'tar.bz2' or file_type == 'tbz':
        opener, mode = tarfile.open, 'r:bz2'
    else:
        raise ValueError("Bad file %s given" % path)

    os.makedirs(to_directory, exist_ok=True)
    with opener(path, mode) as open_obj:
        for member in members:
            kwargs = {"path": to_directory}
            if member is not None:
                kwargs["member"] = member
            getattr(open_obj, method)(**kwargs)


def get_platform_info():
    platform = sys.platform
    if platform == "win32":
        bit = (64 if "64-" in
                     os.popen("wmic os get osarchitecture").read() else 32)
        return {"exe": ".exe", "os": "window", "platform": "win", "bit": bit}
    if platform == "linux":
        bit = 64 if "x86_64" in os.popen("uname -m").read() else 32
        return {"exe": "", "os": "linux", "platform": "linux", "bit": bit}
    if platform == "darwin":
        bit = 64 if "x86_64" in os.popen("uname -m").read() else 32
        return {"exe": "", "os": "macos", "platform": "mac", "bit": bit}


def rename_file(path_to_last_file, new_name, absolute_new_name=False):
    """
    Use to rename or move file
    Args:
        path_to_last_file: the path to the file to be rename or move
        new_name: str, the new path | name
        absolute_new_name: bool, specify if the directory of new_name must be
            use for moving the file

    Returns:
        str, the path to the file renamed or moved

    """
    if os.path.exists(path_to_last_file):
        if not absolute_new_name:
            new_name = os.path.join(
                os.path.dirname(path_to_last_file), os.path.basename(new_name)
            )
        os.rename(path_to_last_file, new_name)
        return new_name


def search_file(file_name, folder_name=None, from_path=os.getcwd(),
                depth=INFINITE):
    """
    Use for seek a specific file
    Args:
        file_name: str, the file name like "test.csv"
        folder_name: str, the file's folder name :"folder_of_test_file/".
        from_path: str, the current folder
        depth: int, maximum depth for seeking, default +infinite

    Returns:
        the file absolute path
    """
    while not os.path.exists(from_path) and not os.path.isdir(from_path):
        from_path = os.path.dirname(from_path)
    try:
        list_dir = os.listdir(from_path)
    except PermissionError:
        return None
    parent_node = os.path.basename(from_path)
    if file_name in list_dir and folder_name in [None, parent_node]:
        path_f = os.path.join(from_path, file_name)
        return os.path.abspath(path_f)

    else:
        if depth <= 0:
            return None
        depth = depth - 1
        for file in list_dir:
            file = os.path.join(from_path, file)
            if os.path.isdir(file):
                path_f = search_file(file_name, folder_name=folder_name,
                                     from_path=file, depth=depth)
                if path_f is not None:
                    return os.path.abspath(path_f)

    return None


def read_json_file(path, default=None) -> Union[dict, list]:
    """
    Use to get content of json file
    Args:
        path: str, the path of json file
        default: default

    Returns:
        json object (list|dict)
    """
    try:
        with open(path) as json_file:
            param = json.load(json_file)
            return param
    except:
        return default


def timer(logger_name=None, verbose=False):
    """
    Evaluate function execution time and print it.
    Args:
        verbose: bool
        logger_name: CustomLogger

    Returns:
        Execution time.

    """

    def inner(func):
        def run(*args, **kwargs):
            """time_wrapper's doc string"""
            start = time.perf_counter()
            result = func(*args, **kwargs)
            time_elapsed = time.perf_counter() - start
            if logger_name:
                logging.getLogger(logger_name).info(
                    f"{func.__name__}, Time: {time_elapsed}"
                )
            if verbose or not logger_name:
                current_time = datetime.datetime.now().replace(microsecond=0)
                print(
                    f"{current_time} INFO: {func.__name__},"
                    f" Time: {time_elapsed}"
                )
            return result

        return run

    return inner


class CustomFileOpen:
    def __init__(self, path, file_type: str = None, limit=None):
        if file_type is None:
            _, file_type = os.path.splitext(path)
        if file_type.startswith("."):
            file_type = file_type[1:]
        self.file_type: str = file_type.lower()
        self.path = path

        if not os.path.exists(self.path):
            file = open(self.path, mode="w")
            file.close()
        self.data = self._get_data(limit=limit)

    def _get_data(self, limit=None):
        if self.file_type in ["txt", "json"]:
            with open(self.path) as file:
                if self.file_type == "json":
                    try:
                        param = json.load(file)
                    except json.decoder.JSONDecodeError:
                        param = {}
                    return param

    def save(self):
        data = self.data
        if self.file_type in ["txt", "json"]:
            if self.file_type == "json":
                data = json.dumps(self.data, indent=4)
            with open(self.path, "w") as file:
                file.writelines(data)


class CustomDateTime:
    SUPPORTED_FORMAT = {"yyyy-mm-dd": "%Y{sep}%m{sep}%d",
                        "dd-mm-yyyy": "%d{sep}%m{sep}%Y"}
    MONTH = {
        1: ["janvier", "january", "janv", "jan", "ja"],
        2: ["février", "fevrier", "february", "fév", "fev", "feb", "fe"],
        3: ["mars", "march", "mar"],
        4: ["avril", "april", "avr", "apr", "ap", "av"],
        5: ["mai", "may"],
        6: ["juin", "june", "jun"],
        7: ["juillet", "july", "jul"],
        8: ["août", "aout", "august", "aug", "ao"],
        9: ["septembre", "september", "sept", "sep"],
        10: ["octobre", "october", "oct"],
        11: ["novembre", "november", "nov", "no"],
        12: ["décembre", "decembre", "december", "dec", "de"]
    }

    def __init__(self, date_value: Union[str, datetime.datetime,
                                         datetime.date] = "now", **kwargs):
        self._source = self._parse(date_value, **kwargs)

    def __call__(self, *args, **kwargs):
        return self._source

    def __getattr__(self, item):
        return getattr(self._source, item)

    def __str__(self):
        return str(self._source)

    def __repr__(self):
        return repr(self._source)

    @staticmethod
    def _parse(date_value: Union[str, datetime.datetime,
                                 datetime.date] = "now",
               **kwargs) -> datetime.datetime:
        if isinstance(date_value, (datetime.datetime, datetime.date)):
            return datetime.datetime.fromisoformat(date_value.isoformat())
        now = datetime.datetime.now()

        args = {k: kwargs.get(k, getattr(now, k))
                for k in ["year", "month", "day", "hour", "minute", "second",
                          "microsecond"]}
        now = datetime.datetime(**args)

        if date_value == "now" or date_value is None:
            date_value = now
        elif isinstance(date_value, str):
            date_value = date_value.strip()
            reg = (r'^(\d{4})[/-](\d{1,2})[/-](\d{1,2})(?:[A-Z]'
                   r'(\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d+))?)?)?$'
                   )
            if re.search(reg, date_value):
                year, month, day, hour, minute, second, micro = re.search(
                    reg, date_value).groups()
                return datetime.datetime(year=int(year),
                                         month=int(month),
                                         day=int(day),
                                         hour=int(hour or 0),
                                         minute=int(minute or 0),
                                         second=int(second or 0),
                                         microsecond=int(micro or 0) * 1000
                                         )
            reg_1 = r'\s(\d{1,2})[/-](\d{1,2})[/-](\d{4})\s'
            reg_0 = r'\s(\d{4})[/-](\d{1,2})[/-](\d{1,2})\s'
            dyear, dmonth, dday, dhour, dminute, dsecond, dmicro = (
                now.year, 1, 1, 0, 0, 0, 0)
            got = True

            year, month, day = dyear, dmonth, dday
            if re.search(reg_1, f" {date_value} "):
                year, month, day = re.search(reg_1,
                                             f" {date_value} ").groups()[::-1]
            elif re.search(reg_0, f" {date_value} "):
                year, month, day = re.search(reg_0, f" {date_value} ").groups()
            else:
                month_ref = {}
                v = ""
                for key, value in CustomDateTime.MONTH.items():
                    for s in value:
                        v += s + "|"
                        month_ref[s] = key
                v = v[:-1]

                reg = r"\s(?:(\d{1,2})\s)?(%s)\s(\d{4})\s" % v

                if re.search(reg, f" {date_value} ", flags=re.I):
                    day, month, year = re.search(reg,
                                                 f" {date_value} ",
                                                 flags=re.I).groups()
                    month = month_ref[month]
                else:
                    got = False

            assert got, f"Date Parsing fail: format not supported ->" \
                        f" {date_value}"
            reg_hour = r"\s(\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d+))?)?\s"
            hour, minute, second, micro = 0, 0, 0, 0

            if re.search(reg_hour, f" {date_value} "):
                hour, minute, second, micro = \
                    re.search(reg_hour, f" {date_value} ").groups()

            date_value = datetime.datetime(year=int(year),
                                           month=int(month),
                                           day=int(day),
                                           hour=int(hour or dhour),
                                           minute=int(minute or dminute),
                                           second=int(second or dsecond),
                                           microsecond=int(micro or
                                                           dmicro) * 1000
                                           )
        return date_value

    def to_string(self, sep="_", microsecond=False, force_time=False,
                  d_format=None):
        t = True
        if not force_time:
            t = (self._source.hour or self._source.minute
                 or self._source.second or self._source.microsecond)
        return self.datetime_as_string(self._source, sep=sep,
                                       microsecond=microsecond, time_=t,
                                       d_format=d_format)

    @staticmethod
    def datetime_as_string(date_time: Union[str, datetime.datetime,
                                            datetime.date] = "now",
                           sep=None, microsecond=False,
                           time_=True, d_format=None):
        """
        Use to get datetime formatting to str
        Args:
            date_time: datetime value
            sep: str
            microsecond: bool, consider microsecond?
            time_: show time
            d_format: str

        Returns:
            str, the datetime str formatted

        """
        if d_format:
            if "-" in d_format:
                sep = "-"
            else:
                sep = "/"
        if not sep:
            sep = "_"
        d_format = CustomDateTime.SUPPORTED_FORMAT.get(
            d_format, "%Y{sep}%m{sep}%d").format(sep=sep)
        current_time = CustomDateTime._parse(date_time)

        if not microsecond:
            current_time.replace(microsecond=0)
        ms = current_time.microsecond
        return current_time.strftime(
            d_format +
            (f"{sep}%H{sep}%M{sep}%S" if time_ else "")
        ) + (f"{sep}{str(ms)[:3]:0>3}" if microsecond and time_ else "")

    @classmethod
    def from_calculation(cls,
                         date_time: Union[str, datetime.datetime,
                                          datetime.date] = "now",
                         minus_or_add: str = None, **kwargs):
        date_time = cls._parse(date_time, **kwargs)
        if isinstance(minus_or_add, str):
            values = re.findall(
                r"([-+])? *(\d+) +(day[s]?|month[s]?|year[s]?|"
                r"week[s]?|hour[s]?|min[s]?|minute[s]?|sec[s]?|second[s]?|"
                r"microsec[s]?|microsecond[s]?)",
                minus_or_add)
            assert len(values), f"Bad value given: '{minus_or_add}'"
            keys = [
                "weeks",
                "days",
                "hours",
                "minutes",
                "seconds",
                "microseconds",
                "years",
                "months",
            ]
            args = {key: 0 for key in keys}
            match = {k[:-1]: k for k in keys}
            for arg in values:
                op, value, item = arg
                if op is None:
                    op = ""
                if item.endswith("s"):
                    item = item[:-1]
                item = match[item]
                args[item] = int(op + value)
            years = args.pop("years")
            months = args.pop("months")

            delta = datetime.timedelta(**args)

            date_time = date_time + delta
            try:
                date_time = date_time.replace(year=date_time.year + years)
            except ValueError:
                assert date_time.month == 2, "An unknown error occurred"
                date_time = date_time.replace(
                    year=date_time.year + years,
                    month=3, day=1) + datetime.timedelta(days=-1)
            try:
                date_time = date_time.replace(month=date_time.month + months)
            except ValueError:
                date_time = date_time.replace(
                    month=date_time.month + months + 1,
                    day=1) + datetime.timedelta(days=-1)

        return cls(date_time)


if __name__ == "__main__":
    print(CustomDateTime('12:10 lundi 13 février 2022')())
