import csv
import os
import re
import time

import chardet
import pandas
import psutil
import io

from kb_package.utils import DatasetFactory
from kb_package import tools


class BIGDatasetFactory:
    TOTAL_VIRTUAL_MEMORY = psutil.virtual_memory().total
    LAST_ELAPSED_EXECUTION_TIME = None
    MEMORY_THRESHOLD = 0.3

    DEFAULT_PART = 50

    @staticmethod
    def is_big_data(path):
        file_size = os.stat(path).st_size
        if file_size / BIGDatasetFactory.TOTAL_VIRTUAL_MEMORY <= BIGDatasetFactory.MEMORY_THRESHOLD:
            return False
        return True

    def __new__(cls, path, columns=None, header=True, sep=None, encoding=None, force_encoding=True, *,
                force_=False, max_nb_line=None,
                **kwargs):
        self = object.__new__(cls)
        if not cls.is_big_data(path):
            if force_:
                self.__force = True
            else:
                return DatasetFactory(path, columns=columns, header=header, sep=sep, encoding=encoding,
                                      force_encoding=force_encoding, **kwargs)
        return self

    def __init__(self, path, columns=None, header=True, sep=None, encoding=None, force_encoding=True, *,
                 force_=False, max_nb_line=None, **kwargs):
        self.__path = path
        encoding = encoding or "utf-8"
        self.__nb_rows = 0
        delimiters = kwargs.pop("delimiters", [',', '\t', ';', ' ', ':'])
        used_sniffer = False
        try:
            if sep is None:
                with open(path, encoding=encoding) as file:
                    sample = [file.readline() for _ in range(10)]
                    try:
                        sep = DatasetFactory._check_delimiter(sample, delimiters)
                        assert sep, ""
                        used_sniffer = True
                    except (csv.Error, AssertionError):
                        sep = None
        except UnicodeDecodeError as exc:
            if not force_encoding:
                raise exc
            min_buffer = int(re.search(r"position (\d+):", str(exc)).groups()[0]) + 100
            with open(path, "rb") as file_from_file_path:
                file_bytes = file_from_file_path.read(min_buffer)
                encoding_proba = chardet.detect(file_bytes).get("encoding", "latin1")
            if encoding != encoding_proba:
                # Logger.warning("We force encoding to:", encoding_proba)
                encoding = encoding_proba
                if not sep or used_sniffer:
                    with open(path, encoding=encoding) as file:
                        sample = [file.readline() for _ in range(10)]
                        try:
                            sep = DatasetFactory._check_delimiter(sample, delimiters)
                            assert sep, ""
                        except (csv.Error, AssertionError):
                            sep = None
            else:
                raise exc
        self.extra_info = tools.Cdict(file_size=os.stat(path).st_size, seek=0, first_row_seek=0)
        self.columns = None
        with open(path, encoding=encoding) as file:
            if header:
                self.__origin_columns = next(csv.reader(io.StringIO(file.readline()), delimiter=sep))
                self.extra_info.first_row_seek = file.tell()
            else:
                self.__origin_columns = columns
            if self.__origin_columns is not None:
                self.columns = DatasetFactory._parse_columns_arg(columns, self.__origin_columns) or \
                               self.__origin_columns
            self.__nb_rows = sum(1 for _ in file)
            # for _ in file:
            #     self.__nb_rows += 1
            self.extra_info["end_file"] = file.tell()

        self.__max_rows_threshold = (
                BIGDatasetFactory.MEMORY_THRESHOLD * self.__nb_rows /
                (self.extra_info.file_size / BIGDatasetFactory.TOTAL_VIRTUAL_MEMORY)
        )
        # print(self.__max_rows_threshold, self.__origin_columns, self.columns)
        self.__max_rows_threshold = int(-(-self.__max_rows_threshold // self.DEFAULT_PART))
        # 16184196.341083076
        self.__sep = sep
        self.__encoding = encoding or "utf-8"
        self.__force_encoding = force_encoding

        self.extra_info.seek, self.__source_temp = self.__get_dataset()
        if self.columns is None:
            self.columns = self.__source_temp.columns

    def __get_dataset(self, seek=None, last_rows=0):
        with open(self.__path, encoding=self.__encoding) as file:
            file.seek(seek or self.extra_info.first_row_seek)
            data = io.StringIO("".join([file.readline() for _ in range(self.__max_rows_threshold)]))

            seek = file.tell()
            data = [row for row in csv.reader(data, delimiter=self.__sep)]
        data = DatasetFactory(pandas.DataFrame(data, columns=self.__origin_columns), columns=self.columns)
        data.index = data.index + last_rows
        return seek, data

    def __repr__(self):
        res = self.__source_temp.__repr__()
        res = re.split(r"^\[", res, flags=re.M)[0][:-1]
        return res + "...\n" + f"[{self.__nb_rows} rows x {len(self.columns)} columns]" \
                               f"\n<kb_package | BIGDatasetFactory>"

    def __str__(self):
        return self.__repr__()

    def __loop(self):
        seek = self.extra_info.first_row_seek or 0
        last_rows = 0
        while seek < self.extra_info.end_file:
            seek, source = self.__get_dataset(seek=seek, last_rows=last_rows)
            last_rows += len(source)
            print("last_rows:", last_rows, "| max:", self.__nb_rows, "seek:", seek, "| max:", self.extra_info.end_file)
            yield source

    def query(self, query, params=None, *, method="parse", reset=False, **kwargs):
        start_time = time.time()
        temp_filename = tools.get_no_filepath("_big_datafactory_temp")
        first = True
        for source in self.__loop():
            res = source.query(query, params=params, method=method, **kwargs)
            res.to_csv(temp_filename, index=not reset, mode="w" if first else "a", header=first)
            first = False
        self.LAST_ELAPSED_EXECUTION_TIME = time.time() - start_time
        return BIGDatasetFactory(temp_filename, index_col=None if reset else 0)

    def apply(self, func, *args, **kwargs):

        pass

    def doublon(self):
        pass

    def group(self):
        pass

    def cmerge(self):
        pass

    def exclude(self):
        pass

    def intersect(self):
        pass

    # __add__, __radd__
    # __setitem__, __setattr__,
    # __delitem__, __delattr__

    def save(self):
        pass

    def sql(self):
        pass

    def sort_values(self):
        pass

    def _parse_col_name_to_index(self, item):
        for i, d in enumerate(self.columns):
            if tools.Var(d) == item:
                return i, d
        raise ValueError("Got bad columns: %s" % item)

    def __getattr__(self, item, default=None):
        # maybe methods or accessible attribute like columns
        try:
            index, name = self._parse_col_name_to_index(item)
            return BIGSeries(self.__path, index, sep=self.__sep, encoding=self.__encoding,
                             nb_rows=self.__nb_rows, threshold=self.__max_rows_threshold,
                             init_seek=self.extra_info.first_row_seek, name=name)
        except ValueError as ex:
            if default:
                return default
            raise ex

    def __getitem__(self, col):
        if isinstance(col, slice):
            raise NotImplementedError("Not implemented slicing __getitem__")
        elif isinstance(col, tuple):
            raise NotImplementedError("Not implemented __getitem__")
        elif isinstance(col, str):
            return self.__getattr__(col)


class BIGSeries:
    def __init__(self, path, col_index=0, sep=None, encoding=None,
                 nb_rows=0, threshold=0, init_seek=0, **kwargs):
        self.__path = path
        self.col_index = col_index
        self.sep = sep
        self.encoding = encoding
        self.nb_rows = nb_rows
        self.threshold = threshold
        self.init_seek = init_seek

        self.pipeline = kwargs.pop("pipeline", [])

        self.kwargs = kwargs

        self.file_seek, self.__source_temp = self.__get_dataset()

    def __get_dataset(self, seek=None, last_rows=0):
        with open(self.__path, encoding=self.encoding) as file:
            file.seek(seek or self.init_seek)
            data = "\n".join([file.readline()[:-1].split(self.sep)[self.col_index]
                              for _ in range(self.threshold)])
            data = io.StringIO(data)
            seek = file.tell()
            data = [row[0] for row in csv.reader(data)]
        data = pandas.Series(data, **self.kwargs)
        data.index = data.index + last_rows
        return seek, data

    def __repr__(self):
        res = self.__source_temp.__repr__().split("\n")
        res, n = "\n".join(res[:-1]), res[-1]
        n = re.sub(r"Length: (\d+)", "Length: " + str(self.nb_rows), n)
        return res + "\n" + n

    def __str__(self):
        return self.__repr__()

    def apply(self):
        pass

    def unique(self):
        pass

    def doublon(self):
        pass

    def append(self):
        pass

    def sort_values(self):
        pass


if __name__ == '__main__':
    pandas.Series()
    file = r"C:\Users\FBYZ6263\Downloads\BASE_EXTRACT_PUSH.csv"
    d = BIGDatasetFactory(file)
    print("loading ok")
    print(d["TYPE_CLIENT_ENDPERIOd"])
    print(d.TYPE_CLIENT_ENDPERIOd)
    # print(d.query("TYPE_CLIENT_ENDPERIOd = %s", ("HYBRID", )))
    print(d.LAST_ELAPSED_EXECUTION_TIME, 257.9834668636322)
