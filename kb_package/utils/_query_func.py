import re

from pandas import isna as isnan
from numpy import vectorize


def like(value, regex, _not=False):
    if isnan(value):
        return _not
    value = str(value)
    res = re.match(regex, value)
    return res is None if _not else res is not None


def lower(value):
    return value.lower() if hasattr(value, "lower") else None


def upper(value):
    return value.upper() if hasattr(value, "upper") else None


def trim(value):
    return value.strip() if hasattr(value, "strip") else None


def replace(old_car, new_car, value):
    if isnan(value):
        return None
    return value.replace(old_car, new_car)


def length(value):
    return 0 if isnan(value) else len(str(value))


def substring(value, start, end=None):
    if isnan(value):
        return None
    if end is None:
        return value[start:]
    return value[start:end]


def to_number(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError) as ex:
        if default is None:
            return default
        raise ex


def to_str(value):
    if isnan(value):
        return None
    return str(value)


def is_in(serie, iterable, _not=False):
    try:
        [d for d in iterable]
    except TypeError:
        iterable = str(iterable)
    if hasattr(serie, "apply"):
        return serie.apply(lambda value: False if isnan(value) else (
            (value in iterable) if not _not else (value not in iterable)))
    from numpy import ndarray
    if isinstance(serie, ndarray) and serie.ndim > 0:
        return list(map(lambda value: False if isnan(value) else (
            (value in iterable) if not _not else (value not in iterable)),  serie))
    return False if isnan(serie) else ((serie in iterable) if not _not else (serie not in iterable))


like = vectorize(like)
lower = vectorize(lower)
upper = vectorize(upper)
length = vectorize(length)
trim = vectorize(trim)
replace = vectorize(replace)
substring = vectorize(substring)
to_str = vectorize(to_str)
to_number = vectorize(to_number)


