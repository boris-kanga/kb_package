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
import typing
import zipfile
from itertools import permutations
from typing import Union
import stat as stat_package

import pandas
import stat
import unicodedata
from collections.abc import Iterable


def add_query_string_to_url(url, params):
    from requests.models import PreparedRequest
    try:

        req = PreparedRequest()
        req.prepare_url(url, params)
        return req.url
    except:
        traceback.print_exc()
    return ""


def force_delete_file(action, name, exc):
    os.chmod(name, stat_package.S_IWRITE)
    os.remove(name)


def safe_eval_math(calculation, params=None, method="expr", result_var="result", **kwargs):
    if not isinstance(params, dict):
        params = {}
    params.update(kwargs)
    for k, v in params.items():
        globals()[k] = v
    if method == "eval":
        return eval(calculation)
    if method == "exec":
        exec(calculation)
        return locals().get(result_var)
    import numexpr
    res = ""
    try:
        res = numexpr.evaluate(calculation)
        return list(res)
    except TypeError:
        return res.item()
    except:
        return ""


def _init_infinite(self, s=1):
    self._s = s > 0


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
REGEX_FRENCH_CHARACTER = r"[A-Za-zÀ-ÖØ-öø-ÿ]"


def remove_accent_from_text(text):
    """
    Strip accents from input String.

    text: The input string.

    returns:
        The processed String.

    """
    text = text.encode("utf-8").decode("utf-8")

    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)


def read_datafile(file_path, drop_duplicates=False, drop_duplicates_on=None):
    if bool(os.stat(file_path).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN):
        dataset = pandas.DataFrame()
    elif os.path.splitext(file_path)[1][1:].lower() in ["xls", "xlsx", "xlsm", "xlsb"]:
        dataset = pandas.read_excel(file_path)
    else:
        with open(file_path, encoding='latin1') as file:
            if ";" in file.readline():
                dataset = pandas.read_csv(file_path, sep=";", encoding='latin1')
            else:
                dataset = pandas.read_csv(file_path, encoding='latin1')

    if drop_duplicates:

        drop_duplicates_on = ([drop_duplicates_on]
                              if isinstance(drop_duplicates_on, str)
                              else drop_duplicates_on)
        drop_duplicates_on = dataset.columns.intersection(drop_duplicates_on)
        if drop_duplicates_on.shape[0]:
            dataset.drop_duplicates(inplace=True,
                                    subset=drop_duplicates_on, ignore_index=True)
    return dataset


def format_var_name(name, sep="_", accent=False):
    name = str(name)
    if accent:
        reg = r"[^ \w\d_]"
    else:
        reg = r'[^ a-zA-Z\d_]'
    name = sep.join([p for p in re.sub(reg, '', str(name), flags=re.I).strip().split(" ") if p])
    return name[1:] if name.startswith(sep) else name


def extract_file(path, member=None, to_directory='.', file_type=None, pwd=None):
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
    assert file_type in ["zip", "tgz", "tar.gz", "tar.bz2", "tbz", "rar"], (
            "Bad file %s given" % path)
    if member is not None:
        method = "extract"
    if file_type == 'zip':
        opener, mode = zipfile.ZipFile, 'r'
    elif file_type == "rar":
        import rarfile
        opener, mode = rarfile.RarFile, 'r'
    elif file_type == 'tar.gz' or file_type == 'tgz':
        opener, mode = tarfile.open, 'r:gz'
    elif file_type == 'tar.bz2' or file_type == 'tbz':
        opener, mode = tarfile.open, 'r:bz2'
    else:
        raise ValueError("Bad file %s given" % path)

    os.makedirs(to_directory, exist_ok=True)
    with opener(path, mode) as open_obj:
        kwargs = {"path": to_directory}
        if pwd is not None:
            kwargs["pwd"] = pwd
        for member in members:
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
        try:
            os.makedirs(os.path.dirname(new_name), exist_ok=True)
        except (OSError, Exception):
            pass
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
        with open(path, encoding="utf-8") as json_file:
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


def many_try(max_try=3, verbose=True, sleep_time=None, logger_name=None):
    def inner(func):
        def run(*args, **kwargs):
            for index_try in range(max_try):
                try:
                    data = func(*args, **kwargs)
                    return data
                except Exception as ex:
                    if verbose or index_try == max_try - 1:
                        if logger_name:
                            logging.getLogger(logger_name).warning(
                                f"Got error: {ex}  when try to execute :{func.__name__}"
                            )
                        else:
                            print("Got error:  when try to execute :", func.__qualname__)
                        if index_try == max_try - 1:
                            raise Exception(ex)
                        if isinstance(sleep_time, (float, int)):
                            time.sleep(sleep_time)
                        print(traceback.format_exc())

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


class Cdict(dict):
    NO_CAST_CONSIDER = True

    def __init__(self, data: Union[dict, list, tuple] = None, keys: Union[list, tuple, str] = None, **kwargs):
        if isinstance(data, dict):
            data.update(kwargs)
        elif isinstance(data, (list, tuple)):
            keys = list(keys or range(len(data)))
            data = {k: dd for k, dd in zip(keys, data)}
        else:
            data = kwargs

        super().__init__(data)
        if self.NO_CAST_CONSIDER:
            self.key_eq = {str(k).lower(): k for k in self.keys()}
        else:
            self.key_eq = {k: k for k in self.keys()}

    def to_json(self, file_path, indent=4):
        self._to_json(self, file_path, indent=indent)

    @staticmethod
    def _to_json(json_data, file_path, indent=4):
        res = json.dumps(json_data, indent=indent)
        with open(file_path, "w") as file:
            file.write(res)

    def get(self, item, default=None):
        key = self.key_eq.get(str(item).lower()) or item
        return super().get(key, default)

    def __getitem__(self, item):
        key = self.key_eq.get(str(item).lower()) or item
        return super().__getitem__(key)

    def __getattr__(self, item, *args):
        key = self.key_eq.get(str(item).lower()) or item
        try:
            return super().__getitem__(key)
        except KeyError:
            if len(args):
                if len(args) == 1:
                    return args[0]
                return args
            raise AttributeError("This attribute %s don't exists for this instance" % item)

    def pop(self, k, *args):
        if self.NO_CAST_CONSIDER:
            k = self.key_eq.pop(str(k).lower())
        return super().pop(k, *args)

    def update(self, other: dict = None, **kwargs):
        other.update(kwargs)
        if self.NO_CAST_CONSIDER:
            temp = {str(k).lower(): k for k in other.keys()}
            keys = set([str(k).lower() for k in other.keys()]).difference(self.key_eq.keys())
            self.key_eq.update({kk: k for kk, k in temp.items() if kk in keys})
        super().update(other)

    def __contains__(self, item):
        return self.key_eq.__contains__(str(item).lower()) or super().__contains__(item)

    def __delitem__(self, k):  # real signature unknown
        """ Delete self[key]. """
        if self.NO_CAST_CONSIDER:
            k = self.key_eq.pop(str(k).lower())
        super().__delitem__(k)

    def __setitem__(self, k, v):  # real signature unknown
        """ Set self[key] to value. """
        if self.NO_CAST_CONSIDER:
            if k not in self:
                self.key_eq[str(k).lower()] = k
        super().__setitem__(k, v)


class CustomDateTime:
    SUPPORTED_FORMAT = {

    }
    SUPPORTED_LANG = ("fr", "en")
    MONTH = {
        1: {"value": ["janvier", "january", "janv", "jan", "ja"], "abr": ("janv", "jan")},
        2: {"value": ["février", "february", "fév", "feb", "fevrier", "fev", "fe"], "abr": ("fév", "feb")},
        3: {"value": ["mars", "march", "mar"], "abr": ("mars", "march")},
        4: {"value": ["avril", "april", "avr", "apr", "ap", "av"], "abr": ("avr", "apr")},
        5: {"value": ["mai", "may"], "abr": ("mai", "may")},
        6: {"value": ["juin", "june", "jun"], "abr": ("juin", "june")},
        7: {"value": ["juillet", "july", "juil", "jul"], "abr": ("juil", "july")},
        8: {"value": ["août", "august", "aout", "aug", "ao"], "abr": ("août", "aug")},
        9: {"value": ["septembre", "september", "sept", "sep"], "abr": ("sept", "sept")},
        10: {"value": ["octobre", "october", "oct"], "abr": ("oct", "oct")},
        11: {"value": ["novembre", "november", "nov", "no"], "abr": ("nov", "nov")},
        12: {"value": ["décembre", "december", "decembre", "dec", "déc", "de"], "abr": ("déc", "dec")}
    }
    WEEKDAYS = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    WEEKDAYS_ABR = ["Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]
    WEEKDAYS_EN = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    WEEKDAYS_EN_ABR = ["Mon", "Tues", "Wed", "Thur", "Fri", "Sat", "Sun"]

    DEFAULT_LANG = "fr"

    def __init__(self, date_value: Union[str, datetime.datetime,
                                         datetime.date] = "now",
                 format_=None, **kwargs):
        self._source = self._parse(date_value, format_=format_, **kwargs)

    def __call__(self, *args, **kwargs):
        return self._source

    def __getattr__(self, item):
        return getattr(self._source, item)

    def __str__(self):
        return str(self._source)

    def __repr__(self):
        return repr(self._source)

    def time_is(self, value):
        try:
            res = re.search(r'(\d{1,2})(?:\s*h?\s*)?(?::\s*(\d{1,2})'
                            r'(?:\s*m?\s*)?)?(?::\s*(\d{1,2})(?:\s*s?\s*)?(?:\.(\d+))?)?',
                            value,
                            flags=re.I | re.S).groups()
            is_equal = False
            this = self._source.time()
            for i, t in enumerate("hour,minutes,second,microsecond".split(",")):
                if res[i] is None:
                    return is_equal
                assert getattr(this, t) == int(res[i])
                is_equal = True

        except (AttributeError, AssertionError):
            return False

    @property
    def date(self):
        return self._source.date()

    @property
    def is_datetime(self):
        return (self._source.minute > 0 or self._source.second > 0 or
                self._source.hour > 0 or self._source.microsecond > 0)

    @staticmethod
    def _get_weekday(date_value: Union[datetime.date, datetime.date], abr=False):
        return (getattr(CustomDateTime, "WEEKDAYS" + (
            "_EN" if CustomDateTime.DEFAULT_LANG.lower() == "en" else ""
        ) + ("_ABR" if abr else ""))[date_value.weekday()])

    @staticmethod
    def _get_month(date_value: Union[datetime.date, datetime.date], abr=False):
        return CustomDateTime.MONTH[date_value.month]["abr" if abr else "value"][
            (1 if CustomDateTime.DEFAULT_LANG.lower() == "en" else 0)]

    @property
    def get_french_weekday(self):
        return self.WEEKDAYS[self._source.weekday()]

    @staticmethod
    def range_date(inf: Union[datetime.date, str], sup: Union[datetime.date, str, int] = None, step=1,
                   freq="day"):
        freq = freq.lower().strip()
        freq = freq[:-1] if len(freq) > 1 and freq[-1] == "s" else freq
        freq = (
                {
                    "d": "day", "j": "day", "day": "day", "jour": "day",
                    "minute": "minute", "min": "minute",
                    "sec": "second", "s": "second", "second": "second",
                    "seconde": "second",
                    "week": "week", "semaine": "week", "sem": "week",
                    "w": "week",
                    "h": "hour", "hour": "hour", "heure": "hour",
                    "millisecond": "millisecond", "mil": "millisecond",
                    "milliseconde": "millisecond",
                    "month": "month", "moi": "month", "m": "month",
                    "y": "year", "annee": "year", "année": "year", "an": "year",
                    "year": "year", "a": "year"
                }.get(freq, "day") + "s")

        if sup is None:
            now = (datetime.datetime.now() if
                   freq in ["hours", "minutes", "seconds", "milliseconds"]
                   else datetime.date.today())
            inf = CustomDateTime(inf)
            inf = (inf() if
                   freq in ["hours", "minutes", "seconds", "milliseconds"]
                   else inf.date)
            if inf < now:
                sup = "now"
            else:
                sup = inf
                inf = "now"

        inf = CustomDateTime(inf)()
        if isinstance(sup, int):
            sup = inf + datetime.timedelta(days=sup)
            if sup < inf:
                step = -1 if not step else -step
        else:
            sup = CustomDateTime(sup)()

        if freq in ["months", "years"]:
            sup = sup.date()
            inf = inf.date()
            while inf <= sup:
                yield inf
                inf = CustomDateTime.from_calculation(inf, minus_or_add="1 " + freq).date
        else:
            if freq == "minutes":
                d = int((sup - inf).total_seconds() / 60) + 1
            elif freq == "hours":
                d = int((sup - inf).total_seconds() / (60 * 60)) + 1
            elif freq == "weeks":
                d = int((sup - inf).days / 7) + 1
            else:
                if freq in ["hours", "minutes", "seconds", "milliseconds"]:
                    pass
                else:
                    sup = sup.date()
                    inf = inf.date()
                d = getattr(sup - inf, freq) + 1
            for i in range(0, d, step or 1):
                yield inf + datetime.timedelta(**{freq: i})

    @staticmethod
    def _parse(date_value: Union[str, datetime.datetime,
                                 datetime.date] = "now",
               ignore_errors=False, default="1900-01-01",
               format_=None,
               **kwargs) -> datetime.datetime:
        if isinstance(date_value, CustomDateTime):
            return date_value._source
        if isinstance(date_value, (datetime.datetime, datetime.date)):
            return datetime.datetime.fromisoformat(date_value.isoformat())
        if isinstance(format_, str):
            return datetime.datetime.strptime(str(date_value), format_)
        now = datetime.datetime.now()

        args = {k: kwargs.get(k, getattr(now, k))
                for k in ["year", "month", "day", "hour", "minute", "second",
                          "microsecond"]}
        now = datetime.datetime(**args)

        if date_value == "now" or date_value is None:
            date_value = now
        elif isinstance(date_value, str):
            date_value = date_value.strip()
            reg = (r'^(\d{4})[/-]?(\d{1,2})[/-]?(\d{1,2})(?:[A-Z]'
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
                    value = value["value"]
                    for s in value:
                        v += s + "|"
                        month_ref[s] = key
                v = v[:-1]
                reg = r"\s(?:(\d{1,2})[\s-]+)?(%s)[\s-]+(\d{2}|\d{4})\s" % v
                if re.search(reg, f" {date_value} ", flags=re.I):
                    day, month, year = re.search(reg,
                                                 f" {date_value} ",
                                                 flags=re.I).groups()
                    if len(year) == 2:
                        if "20" + year <= str(datetime.datetime.now().year):
                            year = "20" + year
                        else:
                            year = "19" + year
                    month = month_ref[month.lower()]
                else:
                    got = False
            try:
                assert got, f"Date Parsing fail: format not supported ->" \
                            f" {date_value}"
            except AssertionError:
                if ignore_errors:
                    default = CustomDateTime._parse(default)
                    day, month, year = default.day, default.month, default.year
                else:
                    raise ValueError(f"Date Parsing fail: format not supported ->"
                                     f" {date_value}")
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

    def to_string(self, sep=None, microsecond=False, force_time=False,
                  d_format=None, t=True):
        if not force_time and t:
            t = (self._source.hour or self._source.minute
                 or self._source.second or self._source.microsecond)
        return self.datetime_as_string(self._source, sep=sep,
                                       microsecond=microsecond, time_=t,
                                       d_format=d_format)

    @staticmethod
    def datetime_as_string(
            date_time: Union[
                str, datetime.datetime,
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

        current_time = CustomDateTime._parse(date_time)
        if isinstance(d_format, str):
            try:
                res = current_time.strftime(d_format)
                assert res != d_format
                return res
            except (ValueError, AssertionError):
                pass
            time_ = False
            d_format = d_format.replace("%", "")
            d_format = re.sub("yyyy", "%Y", d_format, flags=re.I)
            d_format = re.sub("yy", "%y", d_format, flags=re.I)
            d_format = re.sub("aaaa", "%Y", d_format, flags=re.I)
            d_format = re.sub("aa", "%y", d_format, flags=re.I)

            d_format = re.sub("mm", "%m", d_format, flags=re.I)
            d_format = re.sub("dd", "%d", d_format, flags=re.I)
            d_format = re.sub("jj", "%d", d_format, flags=re.I)
            d_format = re.sub("yyyy", "%Y", d_format, flags=re.I)
            d_format = re.sub("day", CustomDateTime._get_weekday(current_time), d_format, flags=re.I)
            d_format = re.sub(r"d\.", CustomDateTime._get_weekday(current_time, abr=True), d_format, flags=re.I)
            d_format = re.sub("jour", CustomDateTime._get_weekday(current_time), d_format, flags=re.I)
            d_format = re.sub(r"j\.", CustomDateTime._get_weekday(current_time, abr=True), d_format, flags=re.I)
            d_format = re.sub("month", CustomDateTime._get_month(current_time), d_format, flags=re.I)
            d_format = re.sub("mois", CustomDateTime._get_month(current_time), d_format, flags=re.I)
            d_format = re.sub(r"m\.", CustomDateTime._get_month(current_time, abr=True), d_format, flags=re.I)

            last_car_is_percent = False
            final_format = ""

            for car in re.split("(?<![A-Za-zÀ-ÖØ-öø-ÿ])(" + REGEX_FRENCH_CHARACTER + ")(?!" +
                                REGEX_FRENCH_CHARACTER + ")", d_format):
                if last_car_is_percent:
                    pass
                else:
                    car = {"d": "%d", "j": "%d", "a": "%Y", "y": "%Y", "m": "%m"}.get(car.lower(), car)
                last_car_is_percent = False
                if car == "%":
                    last_car_is_percent = True
                final_format += car

            d_format = final_format
            # three
            for x in permutations("ymd"):
                d_format = re.sub(''.join(x), "%" + ("%".join([i if i != "y" else "Y" for i in x])), d_format,
                                  flags=re.I)
            for x in "ymd":
                for xx in "ymd":
                    if xx != x:
                        for p in permutations(x + xx):
                            d_format = re.sub(''.join(p), "%" + ("%".join([i if i != "y" else "Y" for i in p])),
                                              d_format,
                                              flags=re.I)

            temp = d_format
            res = re.search(r"%?h{1,2}(\s*[:-\\ ]\s*)%?m{1,2}(\s*[:-\\ ]\s*)%?s{1,2}", temp, flags=re.I)
            final_temp = ""
            while res:
                time_ = False
                sepc = res.groups()
                final_temp += temp[:res.start() + 1] + "%H" + sepc[0] + "%M" + sepc[1] + "%S"
                temp = temp[res.end():]
                res = re.search(r"%?h{1,2}\s*:\s*%?m{1,2}\s*:\s*%?s{1,2}", temp, flags=re.I)
            d_format = final_temp + temp
            if "-" in d_format:
                sep = "-"
            elif "/" in d_format:
                sep = "/"
        else:
            d_format = "%Y{sep}%m{sep}%d"
        if sep is None:
            sep = ""

        if str(d_format).lower() == "normal":
            date_time = CustomDateTime.WEEKDAYS[current_time.weekday()] + " " + \
                        f"{current_time.day:0>2} " + \
                        CustomDateTime.MONTH[current_time.month]["value"][0] + " " + \
                        str(current_time.year)

        else:
            d_format = CustomDateTime.SUPPORTED_FORMAT.get(
                d_format, d_format).format(sep=sep)
            date_time = current_time.strftime(d_format + (f" %H:%M:%S" if time_ else ""))

        if not microsecond:
            current_time.replace(microsecond=0)
        ms = current_time.microsecond
        return date_time + (f":{str(ms)[:3]:0>3}" if microsecond and time_ else "")

    @classmethod
    def from_calculation(cls,
                         date_time: Union[str, datetime.datetime,
                                          datetime.date] = "now",
                         minus_or_add: str = None, **kwargs):

        date_time = cls._parse(date_time, **kwargs)
        if isinstance(minus_or_add, str):
            values = re.findall(
                r"([-+])? *(\d+) +(days?|months?|years?|"
                r"weeks?|hours?|mins?|minutes?|secs?|seconds?|"
                r"microsecs?|microseconds?)",
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
                date_time = date_time.replace(month=(date_time.month + months) % 13 or 1,
                                              year=date_time.year + (date_time.month + months) // 13)
            except ValueError:
                date_time = date_time.replace(
                    month=date_time.month + months + 1,
                    day=1) + datetime.timedelta(days=-1)

        return cls(date_time)


class CModality:
    EQUALITY_THRESHOLD = 0.8

    def __init__(self, *args, values: dict = None, key=None):
        values = (values or {})
        assert isinstance(values, dict), "Bad values given"
        self._values = {}
        modalities = []
        if len(args):
            if len(args) == 1 and isinstance(args[0], Iterable):
                if isinstance(args[0][0], dict):
                    # {modality|name:_, value:}
                    values = {
                        (u.get(key) or u.get("modality") or u.get("name")): u.get("value") or u
                        for u in args[0]
                    }
                    args = [values.keys()]
                elif isinstance(args[0][0], Iterable):
                    # (modality, value1, value2, value3, ...)
                    values = {
                        u[0]: u[1:]
                        for u in args[0]
                    }
                    args = [values.keys()]

                modalities = [str(u) for u in args[0]]
            else:
                if isinstance(args[0], dict):
                    # {modality|name:_, value:}
                    values = {
                        (u.get(key) or u.get("modality") or u.get("name")): u.get("value") or u
                        for u in args
                    }
                    args = values.keys()
                elif isinstance(args[0], Iterable):
                    # (modality, value1, value2, value3, ...)
                    values = {
                        u[0]: u[1:]
                        for u in args
                    }
                    args = values.keys()
                modalities = [str(u) for u in args]

        modalities.sort(key=lambda x: min([len(xx) for xx in x.split("|")]), reverse=True)
        self._modalities = modalities

        self._values = {}
        self._values_no_space = {}
        self._values_no_space_no_accent = {}
        self._values_no_accent = {}
        for k in self._modalities:
            for kk in k.split("|"):
                self._values[kk.lower()] = values.get(k) or values.get(k.lower()) or k
                self._values_no_space[kk.lower().replace("-", "").replace(" ", "")] = self._values.get(kk.lower())
                self._values_no_space_no_accent[
                    remove_accent_from_text(kk.lower().replace("-", "").replace(" ", ""))] = self._values.get(kk.lower())
                self._values_no_accent[remove_accent_from_text(kk.lower())] = self._values.get(kk.lower())

    def _regex(self, remove_space=True, modal=None):
        return re.compile(r"(?:.*?)?(" + "|".join(
            [
                remove_accent_from_text(str(d)
                                        .replace("(", r"\(").replace(")", r"\)")
                                        .replace("-", "").replace(" ", ""))
                if remove_space
                else remove_accent_from_text(str(d)
                                             .replace("(", r"\(").replace(")", r"\)"))
                for d in (modal or self._modalities)
            ]) + r")(?:.*)?", flags=re.I | re.S)

    def get(self, check, default=None, remove_space=True, priority=None):
        check = remove_accent_from_text(check)
        if remove_space:
            check = str(check).replace("-", "").replace(" ", "")
        if priority is None:
            priority = [None]
        if isinstance(priority, dict):
            priority = [priority]
        if BasicTypes.is_iterable(priority):
            rest_modal = set(self._modalities)
            i = 0
            while True:
                mod = None if i >= len(priority) else priority[i]
                if isinstance(mod, dict):
                    modal = [d for d in rest_modal
                             if (isinstance(mod.get("value"), (list, tuple))
                                 and self._values[d.lower()].get(mod.get("key")) in mod.get("value"))
                             or self._values[d.lower()].get(mod.get("key")) == mod.get("value")]
                else:
                    modal = list(rest_modal)

                res = self._regex(remove_space=remove_space, modal=modal).search(check)
                print("got res", res)
                if res:
                    print(res.groups())
                    check = res.groups()[0].lower()
                    return (self._values.get(check) or self._values_no_accent.get(check)
                            or self._values_no_space_no_accent.get(check))
                if i == len(priority):
                    break

                i += 1
                rest_modal = rest_modal.difference(modal)
        check = check.lower()
        candidates = []
        all_modalities = sorted(self._modalities,
                                key=lambda x: (INFINITE
                                               if len(x) in [len(check)-1, len(check), len(check)+1]
                                               else min([len(xx) for xx in x.split("|")])),
                                reverse=True)
        for modality in all_modalities:
            m = modality.lower().strip()
            if remove_space:
                m = m.replace("-", "").replace(" ", "")
            if (
                    len(m) not in range(len(check) - 3, len(check) + 3) and
                    not m.startswith(check) and
                    not check.startswith(m)
            ):
                continue

            if m.startswith(check):
                return self._values.get(modality.lower())
            candidates.append([m, modality])
        res, score, best = CModality.best_similarity(check, candidates, remove_space=remove_space)
        if score >= CModality.EQUALITY_THRESHOLD:

            print("got res: ", res, "->", check, "list: ", best)
        else:
            res = None
        return self._values.get(res.lower() if res is not None else None, default)

    @staticmethod
    def best_similarity(text, candidates, remove_space=True):

        candidates = pandas.DataFrame(candidates, columns=["candidates", "modality"])

        candidates.score = candidates.candidates.apply(lambda candidat: CModality.equal(
            first=candidat, other=text, get=True, remove_space=remove_space))
        best_score = candidates.score.max()
        best = candidates.loc[candidates.score >= best_score, ["modality", "candidates"]]
        # order by first characters
        best = [[k, v] for k, v in zip(best.candidates, best.modality)]
        best = sorted(best, key=lambda x: INFINITE if x[0][0] == text[0] else 0, reverse=True)
        best = sorted(best, key=lambda x: INFINITE if len(x[1]) != len(text) else 0, reverse=True)
        return ([(d[1], best_score, [p[1] for p in best]) for d in best] or [(None, 0, None)])[0]

    @staticmethod
    def equal(first, other, force=True, remove_space=False, get=False):
        res = str(first) == other
        if not force:
            return res if not get else INFINITE
        if res:
            return True if not get else INFINITE
        # prepare texts
        # remove accents
        this = remove_accent_from_text(str(first).lower()).strip()
        other = remove_accent_from_text(str(other.lower())).strip()

        if other == this:
            return True if not get else INFINITE
        # remove special characters
        regex = re.compile(r"""[`~!@#$%^&*()_|+\-=?’;:'",.<>{}\[\]\\/\d]""", flags=re.I | re.S)
        this = regex.sub("", this)
        other = regex.sub("", other)

        if other == this:
            return True if not get else INFINITE
        regex = re.compile("""[^\x00-\x7F]+""", flags=re.I | re.S)
        this = regex.sub("", this)
        other = regex.sub("", other)

        if other == this:
            return True if not get else INFINITE
        # remove_space
        if remove_space:
            regex = re.compile(r"\s+", flags=re.I | re.S)
            this = regex.sub("", this)
            other = regex.sub("", other)
        #
        if other == this:
            return True if not get else INFINITE
        """
        if len(this) > len(other):
            min_size_temp = other
            max_size_temp = this
        else:
            min_size_temp = this
            max_size_temp = other
        """
        lev1 = lev_calculate(other, this)
        if lev1[1] >= CModality.EQUALITY_THRESHOLD:
            # print(this, other, lev1)
            pass
        if get:
            return lev1[1]
        return lev1[1] >= CModality.EQUALITY_THRESHOLD


def lev_calculate(str1, str2):
    dist, r = 0, 0
    try:
        import Levenshtein as lev
        dist = lev.distance(str1, str2)
        r = lev.ratio(str1, str2)
    except ImportError:
        pass
    return dist, r


class ConsoleFormat:
    RED = (255, 0, 0)
    YELLOW = (255, 255, 0)
    BLUE = (0, 0, 255)
    GREEN = (0, 255, 0)
    ORANGE = (255, 102, 0)
    BLACK = (0, 0, 0)
    GRAY = (100, 100, 100)
    NORMAL_FONT = '\033[0m'

    CURRENT_FG = None
    CURRENT_BG = None

    INIT_YET = False

    def __class_getitem__(cls, item):
        return getattr(cls, item, None)

    @staticmethod
    def init():
        os.system("")
        ConsoleFormat.INIT_YET = True

    @staticmethod
    def set_fg(color=None):
        if not ConsoleFormat.INIT_YET:
            ConsoleFormat.init()
        if isinstance(color, str):
            color = getattr(ConsoleFormat, color.upper(), None)
        if isinstance(color, (list, tuple)):
            print(f"\033[38;2;%s;%s;%sm" % color, end="")
        else:
            print(ConsoleFormat.NORMAL_FONT, end="")

    @staticmethod
    def colored(texte, color=None):
        if not ConsoleFormat.INIT_YET:
            ConsoleFormat.init()
        if isinstance(color, str):
            color = getattr(ConsoleFormat, color.upper(), None)
        if isinstance(color, (tuple, list)):
            color = tuple(color[:3])
            return ("\033[38;2;%s;%s;%sm" % color) + str(texte) + ConsoleFormat.NORMAL_FONT
        return texte

    @staticmethod
    def print_table(table):
        cell_left_top = "┌"
        cell_right_top = "┐"
        cell_left_bottom = "└"
        cell_right_bottom = "┘"
        cell_h_join_top = "┬"
        cell_h_join_bottom = "┴"
        cell_vertical_joint_left = "├"
        cell_t_joint = "┼"
        cell_vertical_joint_right = "┤"
        cell_h = "─"
        cell_v = "│"

        if hasattr(table, "shape"):
            size = table.shape[0]
            columns = table.columns
            col_size = [max(table[col].apply(lambda x: len(str(x))).max(), len(col)) for col in columns]

            def looping():
                for _, row in table.iterrows():
                    yield row
        else:
            size = len(table) - 1
            columns = table.pop(0)

            col_size = [max([len(str(x)) for x in [row[i] for row in table]]
                            + [len(columns[i])]) for i in range(len(columns))]

            def looping():
                for row in table:
                    yield row
        lines_temp = []
        # print header
        for i, col in enumerate(columns):
            blank = col_size[i]
            col = str(col) + " " * max(col_size[i] - len(str(col)), 0)
            lines_temp.append(
                ("" if i > 0 else cell_left_top) +
                (cell_h * blank) +
                (cell_h_join_top if i < len(columns) - 1 else cell_right_top)
            )
            lines_temp.append((cell_v if i == 0 else "") + col + cell_v)
            lines_temp.append(
                ("" if i > 0 else (cell_left_bottom if size == 0 else cell_vertical_joint_left)) +
                (cell_h * blank) +
                (
                    (cell_h_join_bottom if size == 0 else cell_t_joint)
                    if i < len(columns) - 1 else
                    (cell_right_bottom if size == 0 else cell_vertical_joint_right)
                )
            )
        print("\n".join(["".join([str(d) for d in lines_temp[k::3]]) for k in range(3)]))
        for row in looping():
            size -= 1
            print(cell_v + (cell_v.join([str(v) + " " * max(col_size[i] - len(str(v)), 0)
                                         for i, v in enumerate(row)])) + cell_v)

            print((cell_vertical_joint_left if size > 0 else cell_left_bottom) + (
                (cell_h_join_bottom if size <= 0 else
                 cell_t_joint).join([cell_h * col_size[i] for i, v in enumerate(row)])
            ) + (cell_vertical_joint_right if size > 0 else cell_right_bottom)
                  )

    @staticmethod
    def reset(got=True):
        pass

    @staticmethod
    def log(text):
        if not ConsoleFormat.INIT_YET:
            ConsoleFormat.init()
        text = " " + text + " "
        blank = r"([^A-Za-zÀ-ÖØ-öø-ÿ\d])"
        text = re.sub(blank + r"(errors?|erreurs?|échecs?|echec)" + blank,
                      r"\1\033[38;2;255;0;0m\2\033[0m\3", text, flags=re.I | re.S)
        text = re.sub(blank + r"(ok|succès|d'accord|succes|success|successful)" + blank,
                      r"\1\033[38;2;0;255;0m\2\033[0m\3", text, flags=re.I | re.S)
        return text[1: -1]

    @staticmethod
    def write(*args, bg=None, fg=None, **kwargs):

        if isinstance(bg, str):
            getattr(ConsoleFormat, bg.upper(), None)
        if isinstance(fg, str):
            getattr(ConsoleFormat, fg.upper(), None)

    @staticmethod
    def progress(current=None, target=None, percent=0,
                 fill="█", empty="-", msg="",
                 finish_msg="✅", decimals=1, size=50):

        if current is not None and target is not None:
            percent = size * abs(current) / max(1, abs(target))
        else:
            if current is not None:
                percent = current * size / 100
        print("|" + fill * round(percent) + empty * round(size - percent) + "|",
              ("{0:." + str(decimals) + "f} %").format(percent * 100 / size), msg, end="\r")
        if percent * 100 / size >= 100:
            print("|" + fill * round(100 * size / 100) + "|", finish_msg, end="\n")


def get_buffer(obj, max_buffer=200, vv=True) -> typing.Union[tuple, typing.Any]:
    i = 0
    if hasattr(obj, "shape"):
        size = max(int(obj.shape[0] / max_buffer), 1)
    else:
        size = max(int(len(obj) / max_buffer), 1)
    for i in range(size):
        if vv:
            yield i / size, obj[i * max_buffer: (i + 1) * max_buffer]
        else:
            yield obj[i * max_buffer: (i + 1) * max_buffer]
    res = obj[(i + 1) * max_buffer:]
    if hasattr(res, "shape"):
        if not res.shape[0]:
            return
    elif not len(res):
        return
    if vv:
        yield (i + 1) / size, res
    else:
        yield res


def format_number(nb=1000, m_sep=" "):
    nb = str(nb).split(".")
    return (
            m_sep.join([x for _, x in get_buffer(nb[0][::-1], 3)])[::-1] +
            ("." + nb[1] if len(nb) > 1 else "")
    )


class BasicTypes:
    EMAIL_RE = r"^([\w\-\.]+@(?:[\w-]+\.)+[\w-]{2,4})$"
    NUMBER_RE = r"^\(?(?:00|\+)?(?:%(indicatif)s\)?)?(\d{1,2})?(\d{8})$"

    @staticmethod
    def pnn_ci(number, plus="+", only_orange=False, permit_fixe=True, *, reseau=None):
        number = str(number).replace(" ", "").replace("-", "")

        nums = {
            "05": ["04", "05", "06", "44", "45", "46", "54", "55", "56",
                   "64", "65", "66", "74", "75", "76", "84", "85", "86",
                   "94", "95", "96"],
            "01": ["01", "02", "03", "40", "41", "42", "43", "50",
                   "51", "52", "53", "70", "71", "72", "73"],

            "07": ["07", "08", "09", "47", "48", "49", "57", "58", "59",
                   "67", "68", "69", "77", "78", "79", "87", "88", "89",
                   "97", "98"],
        }
        reseaux = Cdict(orange="07", mtn="05", moov="01")
        ban_reseau = set()
        if only_orange:
            ban_reseau = {"01", "05"}
        elif reseau is not None:
            if BasicTypes.is_iterable(reseau) and not isinstance(reseau, str):
                ban_reseau = set(k for k in nums if k not in [(reseaux.get(r) or r) for r in reseau])
            else:
                ban_reseau = set(k for k in nums if k != (reseaux.get(reseau) or reseau))
        for k in ban_reseau:
            nums.pop(k, "")
        check = re.match(BasicTypes.NUMBER_RE % {"indicatif": 225}, str(number).split(".")[0].split(",")[0])
        if check:
            check = check.groups()
            extension = check[0]
            if extension is None:
                # numero avec 8 chiffre
                extension = check[1][:2]
                # ancienne numérotation
                if int(extension) in list(range(20, 25)) + list(range(30, 37)):
                    # Fixe
                    if not permit_fixe:
                        return None
                    if check[1][2] == "8" and not only_orange:
                        # MOOV - 21
                        return plus + "22521" + check[1]
                    elif check[1][2] == "0" and not only_orange:
                        # MTN - 25
                        return plus + "22525" + check[1]
                    else:
                        # ORANGE - 27
                        return plus + "22527" + check[1]
                else:
                    # mobile
                    for num in nums:
                        if extension in nums.get(num):
                            return plus + "225" + num + check[1]
            elif extension in ("7,07,27" + ("" if only_orange else ",21,25,1,01,5,05")).split(","):
                return plus + "225" + f"{extension:0>2}" + check[1]
        return None

    @staticmethod
    def is_phone_number(number, only_orange_number=True, permit_fixe=False, indicatif=225):
        indicatif = int(str(indicatif).replace(" ", ""))
        regex_str = BasicTypes.NUMBER_RE % {"indicatif": indicatif}
        number = str(number).replace(" ", "").replace("-", "").split(".")[0].split(",")[0]
        check = re.match(regex_str, number)
        if not check:
            return False
        if str(indicatif) != "225":
            return True
        ext, num = check.groups()
        if ext is None:
            return True
        if only_orange_number:
            if ext not in ("7", "07", *(["27"] if permit_fixe else [])):
                return False
        else:
            if ext not in ("7", "07", "1", "01", "5", "05", *(["25", "21", "27"] if permit_fixe else [])):
                return False
        return True

    @staticmethod
    def is_iterable(value):
        try:
            for _ in value:
                return True
            return True
        except (TypeError, Exception):
            return False

    @staticmethod
    def is_numeric(value, force=False):
        try:
            if isinstance(value, str) and not force:
                return False
            float(value)
            return True
        except (ValueError, TypeError, Exception):
            return False

    @staticmethod
    def is_email(value):
        return re.match(BasicTypes.EMAIL_RE, value)


def replace_quoted_text(text, quotes="\"'"):
    # original_text = text
    modified_text = ""
    strings_replaced = {}

    # string_regex = fr"([{quotes}]).*?\1(?![A-Za-zÀ-ÖØ-öø-ÿ])"
    string_regex = fr"([{quotes}])(?:(?=(\\?))\2.)*?\1"
    res = re.search(string_regex, text, flags=re.S)
    while res is not None:
        strings_replaced["kb_vars_" + str(len(strings_replaced))] = text[res.span()[0]: res.span()[1]]
        modified_text += text[: res.span()[0]] + "kb_vars_" + str(len(strings_replaced) - 1)
        text = text[res.span()[1]:]
        res = re.search(string_regex, text, flags=re.S)
    modified_text += text
    return modified_text, strings_replaced


if __name__ == "__main__":
    d = CustomDateTime('12:50 28 déc 2022')
    print(d.to_string(d_format="y  md"))
    CustomDateTime.datetime_as_string()
