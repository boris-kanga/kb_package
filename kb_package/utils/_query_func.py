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


def in_func(serie, iterable, _not=False):
    try:
        [d for d in iterable]
    except TypeError:
        iterable = str(iterable)
    if hasattr(serie, "apply"):
        return serie.apply(lambda value: (value in iterable) if not _not else (value not in iterable))
    return (serie in iterable) if not _not else (serie not in iterable)


like = vectorize(like)
lower = vectorize(lower)
upper = vectorize(upper)
trim = vectorize(trim)
replace = vectorize(replace)

