from __future__ import annotations

import re
from collections.abc import Iterable
import kb_package.tools as tools
import pandas

INFINITE = tools.INFINITE

__cache__ = {}


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
        try:
            if len(args) == 1:
                data = args[0]
                assert isinstance(data, Iterable), "Bad value given: %s" % data
                data = list(data)
                assert len(data) > 1, "Got Only One argument"
            else:
                data = list(args)
                assert len(data) > 1, "Got Only One argument"
        except AssertionError:
            self._values = None
            return
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
                                                         remove_accent=True, min_length_word=3,
                                                         default=mod_part.lower())
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
                                                     remove_accent=True, min_length_word=3, default=mod_part.lower())
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

    def get(self, check, default=None, threshold=EQUALITY_THRESHOLD, multiple=False):
        if self._values is None:
            if multiple:
                return []
            return default
        original = check
        if original in __cache__:
            if multiple:
                return [__cache__[original]]
            return __cache__[original]
        check = tools.format_var_name(check, remove_accent=True, min_length_word=3, default=check)
        check = str(check).lower()
        best_candidates = []
        for k in (self._key or [None]):
            res = self._regex_obj[k].search(check)
            if res:
                check = res.groups()[0]
                __cache__[original] = self._retrieve(check, key=k)
                if multiple:
                    return [__cache__[original]]
                return __cache__[original]
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
                m = modality.lower()

                if (
                        len(m) not in range(len(check) - 3, len(check) + 3) and
                        not ("_" + check + "_" in "_" + m + "_") and
                        not ("_" + m + "_" in "_" + check + "_") and
                        not len([_p for _p in m.split("_") if _p in check.split("_")])
                ):
                    continue

                if ("_" + check + "_") in ("_" + m + "_"):
                    candidates = []
                    best_candidates.append([m])
                    break
                candidates.append([m])
            if len(candidates):
                res, score, best = CModality.best_similarity(check, candidates, threshold=threshold)
                if score >= threshold:
                    best_candidates.append([res])
        if multiple:
            return [self._retrieve(d[0]) for d in best_candidates]
        if len(best_candidates):
            res, _, _ = CModality.best_similarity(check, best_candidates, threshold=threshold)
            __cache__[original] = self._retrieve(res)
            return __cache__[original]
        __cache__[original] = default
        return __cache__[original]

    @staticmethod
    def best_similarity(text, candidates, threshold=EQUALITY_THRESHOLD):
        candidates = pandas.DataFrame(candidates, columns=["candidates"])

        candidates["score"] = candidates.candidates.apply(lambda candidat: CModality.equal(
            this=candidat, other=text, get=True, threshold=threshold))
        best_score = candidates.score.max()

        best = candidates.loc[candidates.score >= best_score, ["candidates"]]

        # order by first characters
        best = list(best.candidates)
        best = sorted(best, key=lambda x: INFINITE if x[0] == text[0] else 0, reverse=True)
        best = sorted(best, key=lambda x: INFINITE if len(x) != len(text) else 0, reverse=True)
        return ([(d, best_score, best) for d in best] or [(None, 0, None)])[0]

    @staticmethod
    def equal(this, other, force=True, get=False, threshold=EQUALITY_THRESHOLD):
        res = tools.Var(this) == other
        if not force:
            return res if not get else INFINITE
        if res:
            return True if not get else INFINITE

        lev1 = tools.lev_calculate(other, this)

        if get:
            return lev1[1]
        return lev1[1] >= threshold

    def got_dataset_series_modalities(self, series: pandas.Series, key=None, *, max_fils=1000):

        def _apply(d):
            if pandas.isnull(d):
                return d, None
            v = tools.Cdict(self.get(d, default=-1))
            if isinstance(v, int) and not isinstance(v, self.__type):
                return d, None
            if self.__type == dict:
                k = key or self._key
                if isinstance(k, (list, tuple)):
                    k = key[0]
                if k:
                    return d, v[k]
                return d, v
            else:
                return d, v

        unique = {}
        for ss in tools.get_buffer(series.unique(), max_fils, vv=False):
            unique.update({d: v for d, v in tools.concurrent_execution(_apply, len(ss), args=ss)})

        return series.apply(lambda d: unique[d])


if __name__ == '__main__':
    import time

    from kb_package.utils import DatasetFactory
    from kb_package._const import KB_ZONE_CI_PATH

    zoneJson = tools.read_json_file(KB_ZONE_CI_PATH, [])
    dataset = DatasetFactory(r"C:\Users\FBYZ6263\Downloads\adsl_client.csv")

    test = CModality(zoneJson, key=["ua", "search"])
    start_time = time.time()
    print(test.get("sanpedro"))
    dataset["commune"] = test.got_dataset_series_modalities(dataset["ville"], "ua")
    dataset.save()

    print(time.time() - start_time)  # 296
    # print(dataset)
