from __future__ import annotations

import re
import datetime
from itertools import permutations


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

    def __init__(self, date_value: str | datetime.datetime | datetime.date = "now",
                 format_=None, **kwargs):
        self._source = self._parse(date_value, format_=format_, **kwargs)

    def __sub__(self, other):
        if isinstance(other, int):
            return CustomDateTime.from_calculation(self._source, minus_or_add="-" + str(other) + "day")
        return (self.date - CustomDateTime(other).date).days

    def __add__(self, other):
        if isinstance(other, int):
            return CustomDateTime.from_calculation(self._source, minus_or_add=str(other) + "day")
        elif isinstance(other, str):
            return CustomDateTime.from_calculation(self._source, minus_or_add=other)
        return self.date + other

    __radd__ = __add__

    def __call__(self, *args, **kwargs):
        return self._source

    def __getattr__(self, item):
        return getattr(self._source, item)

    def __str__(self):
        return str(self._source)

    def __repr__(self):
        return repr(self._source)

    # OK
    def time_is(self, value) -> bool:
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
    def _get_weekday(date_value: datetime.date | datetime.date, abr=False):
        return (getattr(CustomDateTime, "WEEKDAYS" + (
            "_EN" if CustomDateTime.DEFAULT_LANG.lower() == "en" else ""
        ) + ("_ABR" if abr else ""))[date_value.weekday()])

    @staticmethod
    def _get_month(date_value: datetime.date | datetime.date, abr=False):
        return CustomDateTime.MONTH[date_value.month]["abr" if abr else "value"][
            (1 if CustomDateTime.DEFAULT_LANG.lower() == "en" else 0)]

    @property
    def get_french_weekday(self):
        return self.WEEKDAYS[self._source.weekday()]

    # Ok
    @staticmethod
    def range_date(inf: datetime.date | CustomDateTime | str, sup: datetime.date | CustomDateTime | str | int = None,
                   step=1,
                   freq="day"):
        assert isinstance(step, int) and step != 0, "Bad value of step param. %s given" % (step,)
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
            temp = inf
            sign = 1 if sup > inf and step > 0 else -1
            while True:
                if sign == 1 and temp > sup:
                    return
                elif sign == -1 and temp < sup:
                    return
                yield temp
                temp = CustomDateTime.from_calculation(temp, minus_or_add=str(sign * abs(step)) + freq).date
        else:
            if freq == "minutes":
                d = int((sup - inf).total_seconds() / 60)
            elif freq == "hours":
                d = int((sup - inf).total_seconds() / (60 * 60))
            elif freq == "weeks":
                d = int((sup - inf).days / 7)
            else:
                if freq in ["hours", "minutes", "seconds", "milliseconds"]:
                    pass
                else:
                    # day
                    sup = sup.date()
                    inf = inf.date()
                d = getattr(sup - inf, freq)
            sign = 1 if d > 0 and step > 0 else -1

            d = int(abs(d) + 1)
            for i in range(0, d, abs(step) or 1):
                yield inf + datetime.timedelta(**{freq: i * sign})

    @staticmethod
    def _parse_format(d_format, current_time=None):
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

        # hour
        d_format = re.sub("hh", "%H", d_format, flags=re.I)
        d_format = re.sub("ss", "%S", d_format, flags=re.I)
        if current_time is not None:
            d_format = re.sub("day", CustomDateTime._get_weekday(current_time), d_format, flags=re.I)
            d_format = re.sub(r"d\.", CustomDateTime._get_weekday(current_time, abr=True), d_format, flags=re.I)
            d_format = re.sub("jour", CustomDateTime._get_weekday(current_time), d_format, flags=re.I)
            d_format = re.sub(r"j\.", CustomDateTime._get_weekday(current_time, abr=True), d_format, flags=re.I)
            d_format = re.sub("month", CustomDateTime._get_month(current_time), d_format, flags=re.I)
            d_format = re.sub("mois", CustomDateTime._get_month(current_time), d_format, flags=re.I)
            d_format = re.sub(r"m\.", CustomDateTime._get_month(current_time, abr=True), d_format, flags=re.I)

        last_car_is_percent = False
        final_format = ""

        # one
        for car in re.split(r"\b(\w+)", d_format):
            if last_car_is_percent:
                pass
            else:
                car = {"d": "%d", "j": "%d", "a": "%Y", "y": "%Y", "m": "%m"}.get(car.lower(), car)
            last_car_is_percent = False
            if "%" in car:
                last_car_is_percent = True
            final_format += car

        d_format = final_format

        # three
        for x in permutations("ymd"):
            d_format = re.sub(''.join(x), "%" + ("%".join([i if i != "y" else "Y" for i in x])), d_format,
                              flags=re.I)
        # two
        for x in "ymd":
            for xx in "ymd":
                if xx != x:
                    for p in permutations(x + xx):
                        d_format = re.sub(''.join(p), "%" + ("%".join([i if i != "y" else "Y" for i in p])),
                                          d_format,
                                          flags=re.I)

        temp = d_format
        res = re.search(r"%?h{1,2}(\s*[:-\\_ ]*\s*)?%?m{1,2}((\s*[:-\\_ ]*\s*?)%?s{1,2})?", temp, flags=re.I)
        final_temp = ""

        while res:
            time_ = True
            sepc = res.groups()
            part = temp[:res.start()] + "%H" + sepc[0] + "%M"
            if sepc[1] is not None:
                part += sepc[2] + "%S"
            final_temp += part
            temp = temp[res.end():]
            res = re.search(r"%?h{1,2}(\s*[:-\\_ ]*\s*)?%?m{1,2}((\s*[:-\\_ ]*\s*?)%?s{1,2})?", temp, flags=re.I)

        d_format = final_temp + temp

        return d_format, time_

    # Ok
    @staticmethod
    def _parse(date_value: str | datetime.datetime | datetime.date | CustomDateTime = "now",
               ignore_errors=False, default="1900-01-01",
               format_=None,
               **kwargs) -> datetime.datetime:
        if isinstance(date_value, CustomDateTime):
            return date_value._source
        if isinstance(date_value, (datetime.datetime, datetime.date)):
            return datetime.datetime.fromisoformat(date_value.isoformat())
        if isinstance(format_, str):
            format_ = [format_]
        if isinstance(format_, (list, tuple)):
            for ff in format_:
                try:
                    if "%" not in ff:
                        ff, _ = CustomDateTime._parse_format(ff)
                    return datetime.datetime.strptime(str(date_value), ff)
                except (ValueError, Exception):
                    pass
            if ignore_errors:
                return CustomDateTime(default)()
            raise ValueError("Given arg (%s) doesn't match any format given: %s" % (date_value, format_))
        now = datetime.datetime.now()

        args = {k: kwargs.get(k, getattr(now, k))
                for k in ["year", "month", "day", "hour", "minute", "second",
                          "microsecond"]}
        now = datetime.datetime(**args)

        if date_value == "now" or date_value is None:
            date_value = now
        elif isinstance(date_value, str):

            if len(date_value) < 6 or re.search(r"\d{9,}", date_value) or not re.search(r'\d{2}', date_value):
                if ignore_errors:
                    return CustomDateTime(default)()
                raise ValueError("Bad value given for argument date_value: " + date_value)

            d_format = kwargs.get("d_format")
            equivalent_str_parse_time = {
                "%Y": r"(\d{4})", "%y": r"(\d{2})",
                "%m": r"(\d{2})",
                "%d": r"(\d{1,2})"
            }
            year, month, day = [None] * 3
            dhour, dminute, dsecond, dmicro = [0] * 4
            if d_format is not None:
                if isinstance(d_format, str):
                    d_format = [d_format]
                got = False
                for ff in d_format:
                    orign_ff = ff
                    if "%" not in ff:
                        ff, time_ = CustomDateTime._parse_format(ff)
                    temp = ff
                    for k, v in equivalent_str_parse_time.items():
                        temp = temp.replace(k, v)
                    for car in "AaBb":
                        temp = temp.replace("%" + car, r"(\w+)")
                    for car in "wWjU":
                        temp = temp.replace("%" + car, r"(\d+)")
                    res = re.search("(" + temp + ")", date_value)
                    if res:
                        if "%" in orign_ff:
                            try:
                                temp = datetime.datetime.strptime(res.groups()[0], orign_ff)
                                year, month, day = temp.year, temp.month, temp.day
                                got = True
                                break
                            except (ValueError, Exception):
                                pass
                        else:
                            args = {k: v for k, v in zip(re.findall(r"%([Yymd])", ff), res.groups()[1:])}
                            if "y" in args and "Y" not in args:
                                # make some transformation here
                                args["Y"] = args["y"]
                            year, month, day = args.get("Y", str(now.year)), args.get("m", "01"), args.get("d", "01")
                            if len(year) == 2:
                                if "20" + year <= str(datetime.datetime.now().year):
                                    year = "20" + year
                                else:
                                    year = "19" + year
                            try:
                                temp = datetime.date(int(year), int(month), int(day))
                                year, month, day = temp.year, temp.month, temp.day
                                got = True
                                break
                            except (ValueError, Exception):
                                pass

                if not got:
                    if ignore_errors:
                        return CustomDateTime(default)()
                    raise ValueError("Bad value given for argument date_value: %s for formats: %s" %
                                     (date_value, d_format))
            if year is None:

                date_value = date_value.strip()
                reg = (r'^(\d{4})[_/-]?(\d{1,2})[_/-]?(\d{1,2})(?:[A-Z ]?'
                       r'(\d{1,2})[:_](\d{1,2})(?:[:_](\d{1,2})(?:\.(\d+))?)?[A-Z]?)?$'
                       )
                res = re.search(reg, date_value)

                if res:
                    got = True
                    year, month, day, hour, minute, second, micro = res.groups()
                    if int(month) not in range(1, 13):
                        got = False
                    elif int(year) < 1900:
                        got = False
                    elif int(day) > 31:
                        got = False
                    if got:
                        try:
                            return datetime.datetime(year=int(year),
                                                     month=int(month),
                                                     day=int(day),
                                                     hour=int(hour or 0),
                                                     minute=int(minute or 0),
                                                     second=int(second or 0),
                                                     microsecond=int(micro or 0) * 1000
                                                     )
                        except ValueError:
                            pass
                # try to extract the date from string
                reg_1 = r'\s(\d{1,2})[_/-]?(\d{1,2})[_/-]?(\d{4})\s'
                reg_0 = r'\s(\d{4})[_/-]?(\d{1,2})[_/-]?(\d{1,2})\s'
                dyear, dmonth, dday, dhour, dminute, dsecond, dmicro = (
                    now.year, 1, 1, 0, 0, 0, 0)
                got = False

                year, month, day = dyear, dmonth, dday
                if re.search(reg_1, f" {date_value} "):
                    year, month, day = re.search(reg_1,
                                                 f" {date_value} ").groups()[::-1]
                    if int(month) in range(1, 13) and int(day) <= 31:
                        got = True
                if not got and re.search(reg_0, f" {date_value} "):
                    year, month, day = re.search(reg_0, f" {date_value} ").groups()
                    if int(month) in range(1, 13) and int(day) <= 31:
                        got = True
                if not got:
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
                        got = True
                try:
                    assert got, f"Date Parsing fail: format not supported ->" \
                                f" {repr(date_value)}"
                except AssertionError:
                    if ignore_errors:
                        default = CustomDateTime._parse(default)
                        day, month, year = default.day, default.month, default.year
                    else:
                        raise ValueError(f"Date Parsing fail: format not supported ->"
                                         f" {repr(date_value)}")
            # try to extract hour
            reg_hour = r"\s(\d{1,2})[:_](\d{1,2})(?:[:_](\d{1,2})(?:\.(\d+))?)?(?:\s+(am|pm))?\s"
            hour, minute, second, micro = 0, 0, 0, 0
            reg_hour = re.search(reg_hour, f" {date_value} ", flags=re.I)
            if reg_hour:
                hour, minute, second, micro, am_pm = reg_hour.groups()
                hour = int(hour)
                am_pm = str(am_pm).strip().lower()
                if am_pm == "am" and hour >= 12:
                    hour = hour - 12
                elif am_pm == "pm" and hour < 12:
                    hour = hour + 12
            try:
                date_value = datetime.datetime(year=int(year),
                                               month=int(month),
                                               day=int(day),
                                               hour=int(hour or dhour),
                                               minute=int(minute or dminute),
                                               second=int(second or dsecond),
                                               microsecond=int(micro or
                                                               dmicro) * 1000
                                               )
            except ValueError as ex:
                ex.args = ["Got bad value of date_value: %s. "
                           "After parsing got year=%s, month=%s day=%s" % (date_value, year, month, day)]
                raise ex
        return date_value

    # Ok
    def to_string(self, d_format=None, sep=None, microsecond=False, force_time=False,
                  t=True, intelligent=False, approximative=True):
        if not force_time and t:
            t = (self._source.hour or self._source.minute
                 or self._source.second or self._source.microsecond)
        return self.datetime_as_string(self._source, sep=sep,
                                       microsecond=microsecond, time_=t,
                                       d_format=d_format, intelligent=intelligent,
                                       approximative=approximative)

    # Ok
    @staticmethod
    def datetime_as_string(
            date_time: str | datetime.datetime | datetime.date = "now",
            sep=None, microsecond=False,
            time_=True, d_format=None,
            intelligent=False, approximative=True):
        """
        Use to get datetime formatting to str
        Args:
            date_time: datetime value
            sep: str
            microsecond: bool, consider microsecond?
            time_: show time
            d_format: str
            intelligent: bool
            approximative: bool

        Returns:
            str, the datetime str formatted

        """

        current_time = CustomDateTime._parse(date_time)
        now = CustomDateTime()
        _months = [i for i in CustomDateTime.range_date(current_time.date(), now.date, freq="m")]
        if now() >= current_time:
            _months = _months[1:]
        if intelligent and len(_months) <= 12:
            if CustomDateTime.DEFAULT_LANG == "fr":
                _msg_start = "Il y a " if now() >= current_time else "Dans "
                _msg_end = ""
                hier_text = "Hier à " if now() >= current_time else "Demain à "
            else:
                _msg_start = "" if now() >= current_time else "In "
                _msg_end = "ago" if now() >= current_time else ""
                hier_text = "Yesterday " if now() >= current_time else "Tomorrow "

            if now.date == current_time.date():
                if time_:
                    dts = abs(int((now() - current_time).total_seconds()))
                    _h = int(dts // (60 * 60))
                    if _h > 0:
                        return _msg_start + str(_h) + " h" + " " + _msg_end
                    _m = int(dts // 60)
                    if _m > 0:
                        return _msg_start + str(_m) + "m " + _msg_end
                    return _msg_start + str(dts) + "s " + _msg_end
                else:
                    return "Ce jour" if CustomDateTime.DEFAULT_LANG == "fr" else "This day"
            elif now.date == CustomDateTime.from_calculation(current_time, "+1 day").date:
                return hier_text + \
                       current_time.strftime("%H:%M" if time_ else "")
            elif not len(_months) and CustomDateTime.DEFAULT_LANG == "fr":
                return f"Le {current_time.date().day:0>2}" + current_time.strftime(" à %H:%M" if time_ else "")
            elif abs((now.date - current_time.date()).days) < 30:
                s = "s" if (now.date - current_time.date()).days > 1 else ""
                _jour = (" jour%s" % s) if CustomDateTime.DEFAULT_LANG == "fr" else (" day%s " % s)
                return _msg_start + str(abs((now.date - current_time.date()).days)) + _jour + _msg_end
            elif approximative:
                s = "s" if len(_months) > 1 else ""
                _mois = (" mois" if CustomDateTime.DEFAULT_LANG == "fr" else " month%s " % s)
                return _msg_start + str(len(_months)) + _mois + _msg_end
            else:
                return CustomDateTime.datetime_as_string(current_time, d_format="day month") + \
                       current_time.strftime("%H:%M" if time_ else "")
        elif intelligent and now() >= current_time and now.date.year > current_time.date().year:
            if CustomDateTime.DEFAULT_LANG == "fr":
                return "Il y a longtemps" if now() >= current_time else "Dans un futur lointain"
            return "A long time ago" if now() >= current_time else "In a long time"

        if isinstance(d_format, str):
            if "%" in d_format:
                try:
                    res = current_time.strftime(d_format)
                    assert res != d_format
                    return res
                except (ValueError, AssertionError):
                    pass
            d_format, time_ = CustomDateTime._parse_format(d_format, current_time)
            if "-" in d_format:
                sep = "-"
            elif "/" in d_format:
                sep = "/"
        else:
            d_format = "%Y{sep}%m{sep}%d"
        if sep is None:
            sep = "-"

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

    # Ok
    @classmethod
    def from_calculation(cls,
                         date_time: str | datetime.datetime | datetime.date | CustomDateTime = "now",
                         minus_or_add: str | int | float = None, **kwargs):
        """
        use to generate new date from the passing arg date_time by applying some added day, month, year,
            second, minutes, weeks
        Args:
            date_time: (str, datetime.datetime | datetime.date | CustomDateTime), the init date
            minus_or_add: (str, int, float):
                        when : str
                            it's needed to specify what we want to add. Example
                            >> minus_or_add = "1 day -1month 3 years"; date_time="2023-01-01"
                            (Result) 2025-12-02 00:00:00
                        when : int
                            this is equivalent to f'{minus_or_add} days'
                        when : float
                            Equivalent to f'{minus_or_add} seconds'
        """

        date_time = cls._parse(date_time, **kwargs)
        if isinstance(minus_or_add, int):
            minus_or_add = str(minus_or_add) + " day"
        elif isinstance(minus_or_add, float):
            sign = 1 if minus_or_add >= 0 else -1
            sec, micro = str(abs(minus_or_add)).split(".")
            micro = int(micro) * 1000
            minus_or_add = f"{sign * sec}secs {sign * micro}microseconds"

        if isinstance(minus_or_add, str):
            values = re.findall(
                r"([-+])?\s*(\d+)\s*(days?|months?|years?|"
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
            match.update({"min": "minutes", "sec": "seconds", "microsec": "microseconds"})
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
                while True:
                    if date_time.month + months > 12 or date_time.month + months <= 0:
                        years += 1 if months > 0 else -1
                        months += -12 if months > 0 else 12
                    else:
                        break
                date_time = date_time.replace(month=date_time.month + months, year=date_time.year + years)
            except ValueError:
                # assert date_time.month + months == 2, "An unknown error occurred"
                date_time = date_time.replace(
                    month=date_time.month + months + 1,
                    year=date_time.year + years,
                    day=1) + datetime.timedelta(days=-1)

        return cls(date_time)


if __name__ == '__main__':
    print(CustomDateTime.datetime_as_string("now", d_format="y--md"))
    print(CustomDateTime("202301-01 2:10:10", d_format="%Y%m-%d"))
