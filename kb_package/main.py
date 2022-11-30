import pandas

from kb_package import tools
from kb_package.utils.fdataset import DatasetFactory
import re

localities = DatasetFactory(r"C:\Users\FBYZ6263\Documents\WORK_FOLDER\Broadband\Base_localite.xlsx").dataset

zone = localities.loc[:, ["QUARTIER", "TYPE_QUARTIER", "UA", "DEPART", "REGION"]]


# clone_quartier = []


def test(a):
    # global clone_quartier
    if " ou " in str(a).lower():
        a, b = re.match("^(.*?) ou (.*?)$", a, flags=re.I).groups()
        if len(b) > 2:
            a = a + "|" + b
    return a


zone["SEARCH"] = zone.QUARTIER.apply(test)
zone.loc[zone.SEARCH.isin(zone.UA), "SEARCH"] = zone.loc[zone.SEARCH.isin(zone.UA), ["DEPART", "SEARCH"]].apply(lambda x: " ".join(x), axis=1)
# for quartier in clone_quartier:
zone.sort_values(by="SEARCH", key=lambda col: len(col["SEARCH"]))


for dep in localities.loc[~localities.DEPART.isin(localities.QUARTIER), ["DEPART"]].DEPART.unique():
    _, _dep = next(localities.loc[localities.DEPART == dep].iterrows())
    _dep.loc["QUARTIER"] = dep
    _dep.loc["UA"] = dep
    _dep.loc["TYPE_QUARTIER"] = "Département"
    _dep["SEARCH"] = dep
    _dep = _dep[["QUARTIER", "TYPE_QUARTIER", "UA", "DEPART", "REGION", "SEARCH"]]
    zone = zone.append(_dep, ignore_index=True)

for ua in localities.UA.unique():
    _, _dep = next(localities.loc[localities.UA == ua].iterrows())
    _dep.loc["QUARTIER"] = ua
    _dep.loc["TYPE_QUARTIER"] = "Commune"
    _dep["SEARCH"] = ua
    _dep = _dep[["QUARTIER", "TYPE_QUARTIER", "UA", "DEPART", "REGION", "SEARCH"]]
    zone = zone.append(_dep, ignore_index=True)

print(zone)

other_current_error = [
    {
        "QUARTIER": "ABIDJAN",
        "TYPE_QUARTIER": "Département",
        "UA": "ABIDJAN",
        "DEPART": "ABIDJAN",
        "REGION": "DISTRICT AUTONOME D'ABIDJAN",
        "SEARCH": "abj|abij|0bp00|abd|abi"
    },
    {
        "TYPE_QUARTIER": "Quartier d'Abidjan",
        "DEPART": "ABIDJAN",
        "SEARCH": "2PLTX|2PLT|2PLX",
        "REGION": "DISTRICT AUTONOME D'ABIDJAN",
        "QUARTIER": "DEUX PLATEAUX",
        "UA": "COCODY"
    },
    {
        "DEPART": "ABIDJAN",
        "SEARCH": "ANGRE|TRANCHE",
        "REGION": "DISTRICT AUTONOME D'ABIDJAN",
        "QUARTIER": "ANGRE",
        "UA": "COCODY",
        "TYPE_QUARTIER": "Quartier d'Abidjan",
    },
    {
        "DEPART": "ABIDJAN",
        "SEARCH": "RIVIERA|RIVERA|RIVERRA",
        "REGION": "DISTRICT AUTONOME D'ABIDJAN",
        "QUARTIER": "RIVIERA",
        "UA": "COCODY",
        "TYPE_QUARTIER": "Quartier d'Abidjan",
    },
    {
        "DEPART": "ABIDJAN",
        "SEARCH": "PORTBOU|PORT-BOU|BOUET",
        "REGION": "DISTRICT AUTONOME D'ABIDJAN",
        "QUARTIER": "PORT-BOUET",
        "UA": "PORT-BOUET",
        "TYPE_QUARTIER": "Quartier d'Abidjan",
    },
    {
        "DEPART": "ABIDJAN",
        "SEARCH": "YOP",
        "REGION": "DISTRICT AUTONOME D'ABIDJAN",
        "QUARTIER": "YOPOUGON",
        "UA": "YOPOUGON",
        "TYPE_QUARTIER": "Quartier d'Abidjan",
    },
    {
        "DEPART": "GRAND-BASSAM",
        "SEARCH": "BASSAM",
        "REGION": "SUD-COMOE",
        "QUARTIER": "GRAND-BASSAM",
        "UA": "GRAND-BASSAM",
        "TYPE_QUARTIER": "Sous-prefecture"
    },
    {
        "DEPART": "YAMOUSSOUKRO",
        "SEARCH": "YAKRO|yamous",
        "REGION": "DISTRICT AUTONOME DE YAMOUSSOUKRO",
        "QUARTIER": "YAMOUSSOUKRO",
        "UA": "YAMOUSSOUKRO",
        "TYPE_QUARTIER": "Sous-prefecture"
    }]

zone = zone.append(pandas.DataFrame(other_current_error), ignore_index=True)
zone = zone.to_dict("records")
# print(zone)
tools.Cdict._to_json(zone, "kb_ci_zone_parsing.json")
