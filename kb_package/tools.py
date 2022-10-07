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
import stat as stat_package

import pandas
import psutil
import stat
import Levenshtein as lev
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


def format_var_name(name):
    name = "_".join([p for p in re.sub(r'[^ a-zA-Z\d_]', '', str(name)).strip().lower().split(" ") if p])
    return name[1:] if name.startswith("_") else name


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


def get_performance():
    try:
        v1, v2, v3 = psutil.getloadavg()

        cpu_time_user = psutil.cpu_times().user

        cpu_percent = psutil.cpu_percent(0.2)

        cpu_freq = psutil.cpu_freq().current

        ram_percent = psutil.virtual_memory()[2]

        swap_percent = psutil.swap_memory().percent
    except Exception:
        traceback.print_exc()
        v1, v2, v3 = 0, 0, 0
        cpu_time_user = 0
        cpu_percent = 0
        cpu_freq = 0
        ram_percent = 0
        swap_percent = 0
    data = {
        "cpu_time_user": cpu_time_user,
        "cpu_percent": cpu_percent,
        "cpu_freq": cpu_freq,
        "ram_percent": ram_percent,
        "swap_percent": swap_percent,
        "load_average": f"{v1}-{v2}-{v3}",
    }
    return data


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

    def get(self, item, default=None):
        key = self.key_eq.get(str(item).lower()) or item
        return super().__getitem__(key)

    def __getitem__(self, item):
        key = self.key_eq.get(str(item).lower()) or item
        return super().__getitem__(key)

    def __getattr__(self, item):
        key = self.key_eq.get(str(item).lower()) or item
        return super().__getitem__(key)

    def pop(self, k):
        if self.NO_CAST_CONSIDER:
            k = self.key_eq.pop(str(k).lower())
        return super().pop(k)

    def update(self, other: dict = None, **kwargs):
        other.update(kwargs)
        if self.NO_CAST_CONSIDER:
            temp = {str(k).lower(): k for k in other.keys()}
            keys = set([str(k).lower() for k in other.keys()]).difference(self.key_eq.keys())
            self.key_eq.update({kk: k for kk, k in temp.items() if kk in keys})
        super().update(other)

    def __contains__(self, item):
        return self.key_eq.__contains__(str(item).lower()) or super().__contains__(item)

    def __delitem__(self, k): # real signature unknown
        """ Delete self[key]. """
        if self.NO_CAST_CONSIDER:
            k = self.key_eq.pop(str(k).lower())
        super().__delitem__(k)

    def __setitem__(self, k, v): # real signature unknown
        """ Set self[key] to value. """
        if self.NO_CAST_CONSIDER:
            if k not in self:
                self.key_eq[str(k).lower()] = k
        super().__setitem__(k, v)


class CustomDateTime:
    SUPPORTED_FORMAT = {
        "yyyy-mm-dd": "%Y{sep}%m{sep}%d",
        "yyyy/mm/dd": "%Y{sep}%m{sep}%d",
        "yyyymmdd": "%Y{sep}%m{sep}%d",
        "ymd": "%Y{sep}%m{sep}%d",
        "y-m-d": "%Y{sep}%m{sep}%d",
        "y/m/d": "%Y{sep}%m{sep}%d",
        "dd/mm/yyyy": "%d{sep}%m{sep}%Y",
        "d/m/y": "%d{sep}%m{sep}%Y",
        "dmy": "%d{sep}%m{sep}%Y",
        "d-m-y": "%d{sep}%m{sep}%Y",
        "dd-mm-yyyy": "%d{sep}%m{sep}%Y"}
    MONTH = {
        1: ["janvier", "january", "janv", "jan", "ja"],
        2: ["février", "fevrier", "february", "fév", "fev", "feb", "fe"],
        3: ["mars", "march", "mar"],
        4: ["avril", "april", "avr", "apr", "ap", "av"],
        5: ["mai", "may"],
        6: ["juin", "june", "jun"],
        7: ["juillet", "juil", "july", "jul"],
        8: ["août", "aout", "august", "aug", "ao"],
        9: ["septembre", "september", "sept", "sep"],
        10: ["octobre", "october", "oct"],
        11: ["novembre", "november", "nov", "no"],
        12: ["décembre", "decembre", "december", "dec", "déc", "de"]
    }
    WEEKDAYS = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]

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
    def range_date(inf: Union[datetime.date, str], sup: Union[datetime.date, str, int] = None, step=1):
        if sup is None:
            if CustomDateTime(inf).date() < datetime.date.today():
                sup = "now"
            else:
                sup = inf
                inf = "now"

        inf = CustomDateTime(inf).date()
        if isinstance(sup, int):
            sup = inf + datetime.timedelta(days=sup)
            if sup < inf:
                step = -1 if not step else -step
        else:
            sup = CustomDateTime(sup).date()
        d = (sup - inf).days + 1
        for i in range(0, d, step or 1):
            yield inf + datetime.timedelta(days=i)

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
                    for s in value:
                        v += s + "|"
                        month_ref[s] = key
                v = v[:-1]

                reg = r"\s(?:(\d{1,2})[\s-])?(%s)[\s-](\d{2}|d{4})\s" % v

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

    @property
    def get_french_weekday(self):
        return self.WEEKDAYS[self._source.weekday()]

    def to_string(self, sep=None, microsecond=False, force_time=False,
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
            elif "/" in d_format:
                sep = "/"
        if sep is None:
            sep = ""
        current_time = CustomDateTime._parse(date_time)
        if str(d_format).lower() == "normal":
            date_time = CustomDateTime.WEEKDAYS[current_time.weekday()] + " " + \
                        f"{current_time.day:0>2} " + \
                        CustomDateTime.MONTH[current_time.month][0] + " " + \
                        str(current_time.year)

        else:
            d_format = CustomDateTime.SUPPORTED_FORMAT.get(
                d_format, d_format or "%Y{sep}%m{sep}%d").format(sep=sep)
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

        self._modalities = modalities

        self._values = {k.lower(): values.get(k) or values.get(k.lower()) or k for k in self._modalities}

    def regex(self, remove_space=True):
        return re.compile(r"(?:.*?)?(" + "|".join(
            [
                remove_accent_from_text(str(d).replace("-", "").replace(" ", ""))
                if remove_space
                else remove_accent_from_text(str(d))
                for d in self._modalities
            ]) + r")(?:.*)?", flags=re.I | re.S)

    def get(self, check, default=None, remove_space=True):
        check = remove_accent_from_text(check)
        if remove_space:
            check = str(check).replace("-", "").replace(" ", "")
        res = self.regex(remove_space=remove_space).match(check)
        if res:
            return self._values.get(res.groups()[0].lower())
        check = check.lower()
        candidates = []
        for modality in self._modalities:
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
        lev1 = levCalclulate(other, this)
        if lev1[1] >= CModality.EQUALITY_THRESHOLD:
            # print(this, other, lev1)
            pass
        if get:
            return lev1[1]
        return lev1[1] >= CModality.EQUALITY_THRESHOLD


def levCalclulate(str1, str2):
    Distance = lev.distance(str1, str2)
    Ratio = lev.ratio(str1, str2)

    return Distance, Ratio


def test():
    from kb_package.database.sqlitedb import SQLiteDB

    SQLITE_DB_PATH = r"C:\Users\FBYZ6263\Documents\WORK_FOLDER\Dashboard-project\projects\broadband\broadband_db.sqlite"

    db_object = SQLiteDB(SQLITE_DB_PATH)
    zoneJson = r"C:\Users\FBYZ6263\Documents\WORK_FOLDER\Dashboard-project\projects\broadband\data\zone.json"
    zoneJson = read_json_file(zoneJson, [])

    test = CModality(zoneJson, key="search")
    res = db_object.run_script("""SELECT distinct city FROM parc WHERE date_jour=? group by city 
                                                        order by city""",
                               params=("2022-09-18",),
                               dict_res=True)

    nb_found = 0
    dont_find_modal = ''
    # res = ["a","SAN PEDRO MANZAN IMMEUBLE GRIS", "SAN PËDRO", "SAN--PEDRO", "SANPEDRO", "SANS PEDRO"]
    for d in res:
        if isinstance(d, dict):
            d = d["city"]
        if not isinstance(test.get(d, default=-1), int):
            nb_found += 1
        else:
            dont_find_modal += str(d) + "\n"

    print("ok")
    print("Find :", nb_found, ". Don't find:", len(res) - nb_found)
    print(dont_find_modal)


if __name__ == "__main__":
    # print(CustomDateTime('12:10 lundi 13 février 2022')())
    pass
    d = CustomDateTime("10-sept-20")
    print(d)

    test = Cdict({"t": 1, "p": 3})
    print(test)
    print(test.t, test.T, test["T"])
