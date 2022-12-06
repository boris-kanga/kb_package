# from kb_package.crawler.custom_driver import CustomDriver
import traceback

import pandas

from kb_package.utils.fdataset import DatasetFactory
from kb_package.tools import CModality, Cdict, read_json_file
import requests
from urllib.parse import quote_plus as quote
from bs4 import BeautifulSoup
import time
import concurrent.futures as thread
from kb_package import tools

MAIN_CONTROL = [
    {
        "url": "https://www.annuaireci.com/cote-divoire/fr-FR/search?q=",
        "base": "https://www.annuaireci.com/cote-divoire/fr-FR/",
        "check": ".SearchResults-item a",
        "Email": "[property='email']",
        "Site internet": "[property='url']",
        "Contacts": "[property='telephone']",
        "Adresse": ".BusinessProfile-address",
    },
    {
        "url": "https://www.pratik.ci/annuaires?combine=",
        "base": "https://www.pratik.ci/",
        "check": ".view-content h6 a",
        "Site internet": ".field-name-field-site-web",
        "Contacts": ".field-name-field-telepone-1",
        "Adresse": ".field-name-field-adresse-geo",
        "Email": ".field-name-field-email2",
    },
    {
        "url": 'https://www.goafricaonline.com/annuaire-resultat?type=company&whatWho=',
        "base": 'https://www.goafricaonline.com/',
        "check": "article[data-company-id] a",
        "Site internet": ".tnp-website",
        "Contacts": ".text-13",
        "Adresse": "address",
    },
    {
        "url": 'https://www.goafricaonline.com/annuaire-resultat?type=company&whatWho=',
        "base": 'https://www.goafricaonline.com/',
        "clone": True,
        "check": "article[data-company-id] a",
        "Site internet": ".tnp-website",
        "Contacts": ".text-13",
        "Adresse": "address",
    }

]


def working(index, row, control):
    print("working for index -->", index)
    got = False
    for c in control:
        c = c.copy()
        base = c.pop("base")
        clone = c.pop("clone", False)

        name = str(row.nom_point_vente)
        if name.strip().lower() in ["null", ""] or pandas.isnull(row.nom_point_vente):
            if str(row.sigle_point_vente).strip().lower() in ["null", ""] or pandas.isnull(row.sigle_point_vente):
                return None
            name = str(row.sigle_point_vente)
        elif clone:
            if str(row.sigle_point_vente).strip().lower() in ["null", ""] or pandas.isnull(row.sigle_point_vente):
                continue
            name = str(row.sigle_point_vente) + " (" + name + ")"

        res = BeautifulSoup(requests.get(c.pop("url") + quote(name)).text, "xml")
        link = res.select_one(c.pop("check"))
        if link:
            naming = link.text.strip()
            link = link["href"]
            if not link.startswith("http"):
                link = base + link
            if "goafricaonline" in base:
                if not CModality.equal(row.nom_point_vente, naming, remove_space=True):
                    continue
            res = BeautifulSoup(requests.get(link).text, "xml")
            d = {"index": index}
            for k, v in c.items():
                try:
                    if isinstance(v, (list, tuple)):
                        p = res.select(v[0])[v[1] - 1].text.strip()
                    else:
                        p = res.select_one(v).text.strip()
                except (AttributeError, Exception):
                    p = None
                d[k] = p
            return d
    if not got:
        print("[ -( ] We don't find (", index, ")", row.nom_point_vente)


def processing(file=None, save_file=None, index=None):
    if index is None:
        if file is None:
            file = r"C:\Users\FBYZ6263\Downloads\josias\company.csv"
    else:
        file = "josias_base_" + str(index) + ".csv"
        save_file = "josias_base_" + str(index) + "_export.json"
    if save_file is None:
        save_file = "aide_josias.json"

    if "2" in file or "3" in file:
        for c in MAIN_CONTROL:
            c.pop("Contacts", None)

    if "3" in file:
        for c in MAIN_CONTROL:
            c.pop("Adresse", None)

    company = DatasetFactory(file)

    while True:
        data = read_json_file(save_file, [])
        company = company.loc[max([d["index"] for d in data] + [-1]) + 1:]
        print("Got", company.shape[0], "datas to check")

        if not company.shape[0]:
            break
        if list(company.index)[0] > 0:
            print("reload from row", list(company.index)[0])
        try:
            tools.ConsoleFormat.progress(0)
            for t, part in tools.get_buffer(company, max_buffer=100):
                with thread.ThreadPoolExecutor() as executor:
                    futures = []
                    for index, row in part.iterrows():
                        futures.append(executor.submit(working, index=index, row=row, control=MAIN_CONTROL))
                    for dd in thread.as_completed(futures):
                        if dd is not None:
                            data.append(dd)
                tools.ConsoleFormat.progress(100 * t)
                Cdict._to_json(data, save_file)
        except (requests.exceptions.ConnectionError, Exception) as ex:
            traceback.print_exc()
            if isinstance(ex, requests.exceptions.ConnectionError):
                print("Got connexion error, let's sleep for recovered connexion")
                time.sleep(10)
        finally:
            Cdict._to_json(data, save_file)


if __name__ == '__main__':

    processing(index=1)
