from __future__ import annotations

import re
from collections.abc import Iterable
import kb_package.tools as tools
import pandas

INFINITE = tools.INFINITE


def lev_calculate(str1, str2):
    dist, r = 0, 0
    try:
        import Levenshtein as lev
        dist = lev.distance(str1, str2)
        r = lev.ratio(str1, str2)
    except ImportError:
        pass
    return dist, r


class CModality:
    EQUALITY_THRESHOLD = 0.8

    def __init__(self, *args, key: str | list | tuple = "search"):
        """
        modal = CModality("test", "moi", "boris") ==>
        modal(["test", "moi", "boris"]) ==>
        modal([{"key": "test", ...}, {"key": "moi", ...}, {"key": boris", ...}], key="key") ==>
        modal({"key": "test", ...}, {"key": "moi", ...}, {"key": boris", ...}, key="key")
        """
        assert args, "No modalities got"

        if len(args) == 1:
            data = args[0]
            assert isinstance(data, Iterable), "Bad value given: %s" % data
            data = list(data)
            assert len(data) > 1, "Got Only One argument"
        else:
            data = list(args)
            assert len(data) > 1, "Got Only One argument"
        data = tools.Cdict(data)

        values = {}
        all_modal = []
        self.__type = str
        if isinstance(data[0], dict):
            self.__type = dict
            modalities = {}
            if isinstance(key, str):
                key = [key]
            key = (key or [None])
            for k in key:
                values[k] = {}
                modalities[k] = []
            for index, d in enumerate(data):
                for k in key:
                    mod = (d.get(k) or next(d))
                    for mod_part in mod.split("|"):
                        mod_part = tools.format_var_name(mod_part.lower(),
                                                         remove_accent=True, min_length_word=3)
                        modalities[k].append(mod_part)
                        all_modal.append(mod_part)
                        values[k][mod_part] = data[index]

            for k in key:
                temp = list(set(modalities[k]))
                modalities[k] = sorted(temp, key=lambda x: len(x), reverse=True)
        else:
            key = [None]
            temp = []
            value = {}
            for index, mod in enumerate(data):
                for mod_part in mod.split("|"):
                    mod_part = tools.format_var_name(mod_part.lower(),
                                                     remove_accent=True, min_length_word=3)
                    temp.append(mod_part)
                    all_modal.append(mod_part)
                    value[mod_part] = data[index]
            temp = list(set(temp))
            temp.sort(key=lambda x: len(x), reverse=True)
            modalities = {None: temp}
            values[None] = value
        self._modalities = modalities
        self._data = data
        self._key = key
        self._values = tools.Cdict(values)
        self._regex_obj = {k: self._regex(modal) for k, modal in self._modalities.items()}
        self._all_modal = all_modal

        self._infinite = max([1000*(len(d)+2) for d in self._all_modal])

    def _retrieve(self, modal_item, key=None):
        if key is None and self._key != [None]:
            for k in self._key:
                if modal_item in self._values[k]:
                    return self._values[k].get(modal_item)
        return self._values[key].get(modal_item)

    def _regex(self, modal=None):
        return re.compile(r"\b(" + "|".join(
            [
                re.escape(d)
                for d in (modal or self._modalities)
            ]) + r")\b", flags=re.I | re.S)

    def get(self, check, default=None):
        check = tools.format_var_name(check, remove_accent=True, min_length_word=3)
        print(check)
        best_candidates = []
        for k in (self._key or [None]):
            res = self._regex_obj[k].search(check)
            print(k, res)
            if res:
                check = res.groups()[0]
                return self._retrieve(check, key=k)
            check_len = len(check)
            all_modalities = sorted(self._modalities[k],
                                    key=lambda x: self._infinite if len(x) == check_len else (
                                        self._infinite/2 if len(x) in [check_len - 1, check_len + 1]
                                        else (
                                            self._infinite/3 if len(x) in [check_len - 2, check_len - 2]
                                            else len(x)
                                        )
                                    ),
                                    reverse=True)
            candidates = []

            for modality in all_modalities:

                m = modality

                if (
                        len(m) not in range(len(check) - 3, len(check) + 3) and
                        not ("_" + check + "_" in "_" + m + "_") and
                        not ("_" + m + "_" in "_" + check + "_")
                ):
                    continue

                if ("_" + check + "_") in ("_" + m + "_"):
                    candidates = []
                    best_candidates.append([modality])
                    break
                candidates.append([modality])
            if len(candidates):
                res, score, best = CModality.best_similarity(check, candidates)
                if score >= CModality.EQUALITY_THRESHOLD:
                    best_candidates.append(best)
        if len(best_candidates):
            res, _, _ = CModality.best_similarity(check, best_candidates)
            return self._retrieve(res)
        return default

    @staticmethod
    def best_similarity(text, candidates):
        candidates = pandas.DataFrame(candidates, columns=["candidates"])

        candidates["score"] = candidates.candidates.apply(lambda candidat: CModality.equal(
            this=candidat, other=text, get=True))
        best_score = candidates.score.max()

        best = candidates.loc[candidates.score >= best_score, ["candidates"]]

        # order by first characters
        best = list(best.candidates)
        best = sorted(best, key=lambda x: INFINITE if x[0] == text[0] else 0, reverse=True)
        best = sorted(best, key=lambda x: INFINITE if len(x) != len(text) else 0, reverse=True)
        return ([(d, best_score, best) for d in best] or [(None, 0, None)])[0]

    @staticmethod
    def equal(this, other, force=True, get=False):
        res = tools.Var(this) == other
        if not force:
            return res if not get else INFINITE
        if res:
            return True if not get else INFINITE

        lev1 = lev_calculate(other, this)
        if lev1[1] >= CModality.EQUALITY_THRESHOLD:
            # print(this, other, lev1)
            pass
        if get:
            return lev1[1]
        return lev1[1] >= CModality.EQUALITY_THRESHOLD

    def got_dataset_series_modalities(self, series: pandas.Series, key="search"):

        def test_parse_city(d):
            if pandas.isnull(d):
                return d, None
            if DatasetFactory.is_null(d):
                return d, None
            v = tools.Cdict(self.get(d, default=-1))
            if isinstance(v, int) and not isinstance(v, self.__type):
                return d, None
            if self.__type == dict:
                if key:
                    return d, v[key]
                return d, v
            else:
                return d, v

        ss = series.unique()
        ss = {d: v for d, v in tools.concurrent_execution(test_parse_city, len(ss), args=ss)}

        return series.apply(lambda d: ss[d])


if __name__ == '__main__':
    import time

    from kb_package.utils import DatasetFactory
    from kb_package._const import KB_ZONE_CI_PATH

    zoneJson = tools.read_json_file(KB_ZONE_CI_PATH, [])
    # print(zoneJson)
    # dataset = DatasetFactory(r"C:\Users\FBYZ6263\Downloads\base_parc_4G_fixe.csv")

    test = CModality(zoneJson, key=["ua", "search"])
    start_time = time.time()
    # dataset["TEST"] = test.got_dataset_series_modalities(dataset["ville"], "ua")

    print(test.get("sanpedro"))

    print(time.time() - start_time)  # 296
    # print(dataset)
