# -*- coding: utf-8 -*-
"""
All generals customs tools we develop.
they can be use in all the project
"""
import inspect
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
from typing import Union
import stat as stat_package
import shutil
import tempfile
import keyword

import unicodedata

import concurrent.futures as thread

from kb_package.utils.custom_datetime import CustomDateTime

try:
    import Levenshtein as leven
except ImportError:
    leven = type("Levenshtein", (), {"distance": lambda x, y: 0, "ratio": lambda x, y: 0})


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


def concurrent_execution(func, thread_nb=100, wait_for=True, max_workers=os.cpu_count() + 4, *, args=None, kwargs=None):
    with thread.ThreadPoolExecutor(max_workers=max_workers) as executor:
        data = []
        futures = []
        for index in range(thread_nb):
            _kwargs = kwargs or {}
            _args = args
            if callable(kwargs):
                _kwargs = kwargs(index)
            if callable(args):
                _args = args(index)
                if not BasicTypes.is_iterable(_args):
                    _args = (_args,)
            elif isinstance(args, str) or not getattr(args, "__getitem__", None):
                _args = (args,)
            elif getattr(args, "__getitem__", None):
                _args = (args[index], )

            futures.append(executor.submit(func, *_args, **_kwargs))
        if not wait_for:
            return
        for dd in thread.as_completed(futures):
            if dd is not None:
                data.append(dd.result())
    return data


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


# check for 1.8e308 -> in python it is the inf
# all number [-5,0 ⨉ 10 -324, 5,0 ⨉ 10 -324] == 0

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


def get_no_filepath(filepath):
    index = 1
    f, ext = os.path.splitext(filepath)
    while os.path.exists(filepath):
        index += 1
        filepath = f + "_" + str(index) + ext
    return filepath


def lev_calculate(str1, str2):
    dist = leven.distance(str1, str2)
    r = leven.ratio(str1, str2)
    return dist, r


def format_var_name(name, sep="_", accent=False, permit_char=None, default="var", remove_accent=False,
                    min_length_word=1, no_case=False, blacklist=keyword.kwlist):
    origin = str(name).strip()
    name = origin
    if no_case:
        name = name.lower()
    if accent:
        reg = r" \w\d_"
    else:
        if remove_accent:
            try:
                name = remove_accent_from_text(name)
            except (ValueError, Exception):
                pass
        reg = r' a-zA-Z\d'
    reg += re.escape("".join(permit_char or ""))
    reg = '[^' + reg + "]"
    name = sep.join([p for p in re.sub(reg, ' ', name, flags=re.I).strip().split() if p and len(p) >= min_length_word])
    name = sep.join([x for x in re.split(r"^(\d+)", name)[::-1] if x])
    if re.match(r"^\d*$", name):
        return str(default)

    if blacklist:
        if no_case:
            blacklist = [str(d).lower() for d in blacklist]
        while name in blacklist:
            name += "_"
    return name


class Var(str):
    def __new__(cls, *args, **kwargs):
        default = kwargs.pop("default", None)

        remove_accent = kwargs.pop("remove_accent", True)
        ratio = kwargs.pop("eq_ratio", 0.8)
        dist = kwargs.pop("eq_dist", 1)
        force = kwargs.pop("force", False)
        no_case = kwargs.pop("no_case", True)

        self = str.__new__(cls, *args, **kwargs)

        self._force = force
        self._ratio = ratio
        self._dist = dist
        self._remove_accent = remove_accent
        self._no_case = no_case

        # self = cls(*args, **kwargs)
        self._good = format_var_name(self, default=default or self, remove_accent=remove_accent).strip("_")

        if self._no_case:
            self._good = self._good.lower()
        return self

    def __eq__(self, other):
        try:
            if not isinstance(other, Var):
                other = format_var_name(other, default=other, remove_accent=self._remove_accent).strip("_")
                if self._no_case:
                    other = other.lower()
        except AttributeError:
            return self == other
        res = self._good == other
        if res or not self._force:
            return res
        dist, ratio = lev_calculate(self._good, other)
        return dist >= (self._dist or 0) and ratio >= self._ratio

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return super().__hash__()


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


def image_to_base64(path):
    import mimetypes
    import base64
    content_type = mimetypes.guess_type(path)[0] or "images/jpeg"
    with open(path, "rb") as file:
        return f"data:{content_type};charset=utf-8;base64," + \
               base64.b64encode(file.read()).decode("utf-8")


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


def rename_file(path_to_last_file, new_name, *, use_origin_folder=False):
    """
    Use to rename or move file
    Args:
        path_to_last_file: the path to the file to be rename or move
        new_name: str, the new path | name
        use_origin_folder: bool, specify if the directory of new_name must be
            use for moving the file

    Returns:
        str, the path to the file renamed or moved

    """
    if os.path.exists(path_to_last_file):
        last_folder = os.path.dirname(path_to_last_file)
        if use_origin_folder:
            new_name = os.path.join(
                last_folder, os.path.basename(new_name)
            )
        try:
            os.makedirs(os.path.dirname(new_name), exist_ok=True)
        except (OSError, Exception):
            pass
        shutil.move(path_to_last_file, new_name)
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
                current_time = CustomDateTime().to_string(d_format="yyyy-mm-dd h:m:s")
                print(
                    f"{current_time} INFO: {func.__name__},"
                    f" Time: {time_elapsed}"
                )
            return result

        return run

    return inner


def many_try(max_try=3, verbose=True, sleep_time=None, logger_name=None, error_got=None):
    def inner(func):
        log = lambda *args, **kwargs: print(*args, **kwargs)
        if verbose:
            if logger_name is not None:
                log = lambda *args, **kwargs: logging.getLogger(logger_name).warning(*args, **kwargs)
            else:
                log = lambda *args, **kwargs: print(*args, ":", kwargs)

        def run(*args, **kwargs):
            for index_try in range(max_try):
                try:
                    data = func(*args, **kwargs)
                    return data
                except Exception as ex:
                    if error_got is not None:
                        if isinstance(error_got, str):
                            if str(ex) == error_got:
                                pass
                            else:
                                index_try = max_try
                        elif inspect.isclass(error_got) and isinstance(ex, error_got):
                            pass
                        else:
                            index_try = max_try
                    if index_try < max_try - 1:
                        log("ignore error %s" % ex)
                        if isinstance(sleep_time, (float, int)):
                            time.sleep(sleep_time)
                    else:
                        raise ex

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

    def __new__(cls, *args, **kwargs):
        data = {}
        if len(args) == 1:
            data = args[0]
        elif len(args) > 1:
            data = args
        __no_parse_string = kwargs.get("_Cdict__no_parse_string", False)
        if isinstance(data, str) and not __no_parse_string:
            got = False
            try:
                if os.path.exists(data):
                    data = read_json_file(data, {})
                    got = True
            except (FileNotFoundError, FileExistsError, Exception):
                pass
            if not got:
                try:
                    data = json.loads(data)
                except json.decoder.JSONDecodeError:
                    return data
            if isinstance(data, list):
                return [cls(d, _Cdict__no_parse_string=True) for d in data]
            else:
                return cls(data, **kwargs)
        elif data is None:
            return None
        elif isinstance(data, str):
            return data
        elif isinstance(data, (int, float)):
            return data
        if isinstance(data, (list, tuple)):
            return type(data)([cls(d, _Cdict__no_parse_string=True) for d in data])
        return dict.__new__(cls)

    def __init__(self, *args, **kwargs):
        kwargs.pop("_Cdict__no_parse_string", None)
        data = {}
        if len(args) == 1:
            data = args[0]
        elif len(args) > 1:
            data = args
        self.__file_name = None
        if isinstance(data, dict):
            data.update(kwargs)
        else:
            data = kwargs

        for k in data:
            data[k] = Cdict(data[k], _Cdict__no_parse_string=True)
            # if isinstance(data[k], dict):
            #    data[k] = Cdict(data[k])

        super().__init__(data)

    def __parse_item(self, item):
        for i in self.keys():
            if self.NO_CAST_CONSIDER:
                if Var(i) == item:
                    return i
            elif i == item:
                return i
        return item

    def to_json(self, file_path=None, indent=4):
        self._to_json(self, file_path or self.__file_name, indent=indent)

    @staticmethod
    def _to_json(json_data, file_path, indent=4):
        res = json.dumps(json_data, indent=indent)
        with open(file_path, "w") as file:
            file.write(res)

    def get(self, item, default=None):
        key = self.__parse_item(item)
        return super().get(key, default)

    def __getitem__(self, item):
        key = self.__parse_item(item)
        return super().__getitem__(key)

    def __getattr__(self, item, *args):
        key = self.__parse_item(item)
        try:
            return super().__getitem__(key)
        except KeyError:
            if len(args):
                if len(args) == 1:
                    return args[0]
                return args
            raise AttributeError("This attribute %s don't exists for this instance" % item)

    def pop(self, k, *args):
        k = self.__parse_item(k)
        return super().pop(k, *args)

    def __contains__(self, item):
        return item in [Var(k) if self.NO_CAST_CONSIDER else k for k in self.keys()] or super().__contains__(item)

    def __delitem__(self, k):
        """ Delete self[key]. """
        k = self.__parse_item(k)
        super().__delitem__(k)

    def __setitem__(self, k, v):
        """ Set self[key] to value. """
        k = self.__parse_item(k)
        v = Cdict(v, _Cdict__no_parse_string=True)
        super().__setitem__(k, v)

    def __setattr__(self, key, value):
        if key in ["_Cdict" + c for c in ["__file_name"]]:
            super().__setattr__(key, value)
            return
        value = Cdict(value, _Cdict__no_parse_string=True)
        self.__setitem__(key, value)


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
    def print_table(table, car=None):
        cell_left_top = car or "┌"
        cell_right_top = car or "┐"
        cell_left_bottom = car or "└"
        cell_right_bottom = car or "┘"
        cell_h_join_top = car or "┬"
        cell_h_join_bottom = car or "┴"
        cell_vertical_joint_left = car or "├"
        cell_t_joint = car or "┼"
        cell_vertical_joint_right = car or "┤"
        cell_h = car or "─"
        cell_v = car or "│"

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
        text = re.sub(r"\b(errors?|erreurs?|échecs?|echec)\b",
                      r"\1\033[38;2;255;0;0m\2\033[0m\3", text, flags=re.I | re.S)
        text = re.sub(r"\b(ok|succès|d'accord|succes|success|successful)\b",
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
                 finish_msg=" 100%", decimals=1, size=50, _print=print):

        if current is not None and target is not None:
            percent = size * abs(current) / max(1, abs(target))
        else:
            if current is not None:
                percent = current * size / 100
        _print("|" + fill * round(percent) + empty * round(size - percent) + "|",
               ("{0:." + str(decimals) + "f} %").format(percent * 100 / size), msg, end="\r")
        if percent * 100 / size >= 100:
            _print("|" + fill * round(100 * size / 100) + "|", finish_msg, end="\n")


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
    LINK_RE = r'^(?:https?://)?(?:www\.)?[-a-zA-Z0-9@:%._+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b' \
              r'([-a-zA-Z0-9()@:%_+.~#?&/=]*)$'
    REGEX_FRENCH_CHARACTER = r"[A-Za-zÀ-ÖØ-öø-ÿ]"

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

    @staticmethod
    def is_link(value):
        return re.match(BasicTypes.LINK_RE, value, flags=re.I)


def _get_new_kb_text(string, root="kb_vars"):
    """
    Internal utils function
    """
    i = 0
    temp = root
    part1, part2 = root[:int(len(root)/2)], root[int(len(root)/2):]
    while temp in string:
        i += 1
        temp = part1 + "_" + str(i) + "_" + part2
    return temp


def replace_quoted_text(text, quotes=None, preserve=True, no_preserve_value=""):
    if quotes is None:
        quotes = "[\"']"
    elif isinstance(quotes, str):
        quotes = re.escape(quotes)
    elif BasicTypes.is_iterable(quotes):
        quotes = "[" + ("|".join([re.escape(str(d)) for d in quotes])) + "]"
    # original_text = text
    modified_text = ""
    strings_replaced = {}
    base_var_name = _get_new_kb_text(text) + "_"
    # string_regex = fr"([{quotes}]).*?\1(?![A-Za-zÀ-ÖØ-öø-ÿ])"
    string_regex = fr"({quotes})(?:(?=(\\?))\2.)*?\1"
    res = re.search(string_regex, text, flags=re.S)
    while res is not None:
        index = base_var_name + f"{len(strings_replaced)}" + "_"
        strings_replaced[index] = text[res.span()[0]: res.span()[1]]
        modified_text += text[: res.span()[0]] + (index if preserve else no_preserve_value)
        text = text[res.span()[1]:]
        res = re.search(string_regex, text, flags=re.S)
    modified_text += text
    return modified_text, strings_replaced


def extract_structure(text, symbol_start, symbol_end=None, maximum_deep=INFINITE, only_content=False,
                      flags=re.S, sep="", preserve=True, *,
                      internal_var=None):
    """
    use regex to split text using symbol_start and symbol_end.
        Possibility tu split using logic like
            start = {% (for|end) text %}
            end = {% end\1 %} -- where \1 refer to the extracted element in start regex

    """
    if symbol_end is None:
        symbol_end = symbol_start
        maximum_deep = 1
    if internal_var is None:
        # calcul de no_exists_character
        no_exists_character = _get_new_kb_text(text) + "_"
    else:
        no_exists_character = internal_var + "_"

    def split_func(reg, string):
        original_text = string
        split_text = []
        try:
            res = re.search(reg, string, flags=flags)
            while res is not None:
                # before
                if res.span() == (0, 0):
                    # show warning bad split arg given
                    return [string]
                split_text.append(string[:res.span()[0]])
                #
                groups = res.groups()
                match = string[res.span()[0]: res.span()[1]]
                if len(groups):
                    match = (match, *groups)
                split_text.append(match)
                string = string[res.span()[1]:]
                res = re.search(reg, string, flags=flags)

            if string:
                split_text.append(string)
        except re.error:
            return [original_text]
        return split_text

    deep = 0

    text_part = split_func(symbol_start, text)
    _structures = {}
    current_structure = ""
    structure_content = ""
    current_args = ()
    epsilon = ""

    for i, part in enumerate(text_part):
        # print("[", i, "] showing part", part.__repr__(), deep)
        if isinstance(part, (tuple, list)):
            part = list(part)
            epsilon += part[0]
        else:
            epsilon += part

        if i % 2 == 1:
            # part is a new symbol_start
            args = ()
            if isinstance(part, list):
                args = part[1:]
                part = part[0]
            if not deep:
                structure_content = ""
                current_structure = part
                current_args = tuple(args)
            else:
                structure_content += part
                current_structure += part
            deep += 1
            # print("---new part", repr(part), current_args)

        else:
            if len(current_args):
                consider_symbol_end = ""
                for car in symbol_end:
                    _is_var = list(filter(lambda x: chr(x) == car, range(1, min(len(current_args) + 1, 9))))
                    if _is_var:
                        car = re.escape(current_args[_is_var[0] - 1])
                    consider_symbol_end += car
            else:
                consider_symbol_end = symbol_end
            # print("final end reg", consider_symbol_end)
            reach = False
            for ii, end_part in enumerate(split_func(consider_symbol_end, part)):
                if isinstance(end_part, (tuple, list)):
                    end_part = end_part[0]
                if not reach:
                    current_structure += end_part
                if ii % 2 == 1:
                    # end_part is a symbol_end
                    deep -= 1
                if deep:
                    structure_content += end_part
                else:
                    reach = True

                # print("==> [", ii, "]", "end part", repr(end_part), "deep==", deep, ", content",
                #          repr(structure_content))
            # print("at all structure==", repr(current_structure))
        if deep == 0 and i > 0:
            index = no_exists_character + str(len(_structures))
            epsilon = epsilon.replace(current_structure, (index + "__" + sep) if preserve else "", 1)
            _structure = current_structure if not only_content else structure_content
            if maximum_deep > 1:
                eps, sub_structures = extract_structure(structure_content, symbol_start, symbol_end,
                                                        maximum_deep=maximum_deep - 1, flags=flags,
                                                        sep=sep,
                                                        internal_var=index, only_content=only_content)
                if sub_structures:
                    # internal structure found
                    _structures[index + "__"] = _structure.replace(structure_content, eps, 1)
                    _structures.update(sub_structures)
                else:
                    _structures[index + "__"] = _structure
            else:
                _structures[index + "__"] = _structure
    return epsilon, _structures


class CTemporaryFile:
    def __init__(self, mode='wb', destination_path=None, ext=None, delete=True):
        self._mode = mode
        self._delete = delete
        self._name = None
        if isinstance(ext, str):
            if not ext.startswith("."):
                ext = "." + ext
        else:
            ext = ""
        self._ext = ext
        if destination_path:
            self._name = str(destination_path).replace("\\", "/")
            while self._name.endswith("/"):
                self._name = self._name[:-1]
            self._name = os.path.realpath(self._name)
            logging.warning("Make sure that it's safe to use this file like temp file: " + self._name)

    def __enter__(self):
        # Generate a random temporary file name
        if self._name:
            if os.path.isdir(self._name):
                self._name = os.path.join(self._name, os.urandom(24).hex())
        file_name = self._name or os.path.join(tempfile.gettempdir(), os.urandom(24).hex() + self._ext)
        # Ensure the file is created
        open(file_name, "x").close()
        # Open the file in the given mode
        self._tempFile = open(file_name, self._mode)
        return self._tempFile

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._tempFile.close()
            if self._delete:
                os.remove(self._tempFile.name)
        except (OSError, FileNotFoundError, FileExistsError):
            pass
        finally:
            try:
                if self._delete:
                    os.remove(self._tempFile.name)
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    print(extract_structure("""
    <p>Blabla </p>{% if test %} <div>  {% if ok %}OK {% endif %} </div> {% endif %} <span>Fin</span>
    """,
                            symbol_start=r"{%\s(if|for).+?\s%}",
                            symbol_end="{%\send\1\s%}",
                            maximum_deep=2, flags=re.S | re.I))
