import re
from numpy import vectorize


def like(value, regex, _not=False):
    res = re.match(regex, value)
    return res is None if _not else res is not None


def lower(value):
    return value.lower()


def upper(value):
    return value.upper()


def trim(value):
    return value.strip()


def replace(old_car, new_car, value):
    return value.replace(old_car, new_car)


def length(value):
    return len(str(value))


def substring(value, start, end=None):
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
    return str(value)


def is_in(serie, iterable, _not=False):
    try:
        [d for d in iterable]
    except TypeError:
        iterable = str(iterable)
    if hasattr(serie, "apply"):
        return serie.apply(lambda value: (value in iterable) if not _not else (value not in iterable))
    from numpy import ndarray
    if isinstance(serie, ndarray) and serie.ndim > 0:
        return list(map(lambda value: (value in iterable) if not _not else (value not in iterable),  serie))
    return (serie in iterable) if not _not else (serie not in iterable)


like = vectorize(like)
lower = vectorize(lower)
upper = vectorize(upper)
length = vectorize(length)
trim = vectorize(trim)
replace = vectorize(replace)
substring = vectorize(substring)
to_str = vectorize(to_str)
to_number = vectorize(to_number)


