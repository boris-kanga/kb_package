# from kb_package.crawler.custom_driver import CustomDriver
import traceback

from kb_package.utils.fdataset import DatasetFactory
from kb_package.tools import CModality, Cdict, read_json_file
import requests
from urllib.parse import quote
from bs4 import BeautifulSoup
import time

if __name__ == '__main__':
    file = r"C:\Users\FBYZ6263\Downloads\josias\company.csv"

    company = DatasetFactory(file)

    control = [
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
        },
        {
            "url": "https://www.pratik.ci/annuaires?combine=",
            "base": "https://www.pratik.ci/",
            "check": ".view-content h6 a",
            "Site internet": ".field-name-field-site-web",
            "Contacts": ".field-name-field-telepone-1",
            "Adresse": ".field-name-field-adresse-geo",
            "Email": ".field-name-field-email2",
        }
    ]

    while True:
        data = read_json_file("aide_josias.json", [])
        company = company.loc[max([d["index"] for d in data]+[-1])+1:]

        if not company.shape[0]:
            break
        if list(company.index)[0] > 0:
            print("reload from row", list(company.index)[0])
        try:
            for index, row in company.iterrows():
                print("working for index -->", index)
                got = False
                for c in control:
                    c = c.copy()
                    base = c.pop("base")
                    clone = c.pop("clone", False)
                    name = str(row.nom_point_vente)
                    if not len(name.strip()):
                        name = str(row.sigle_point_vente)
                    elif clone:
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
                        data.append(d)
                        got = True
                        break
                if not got:
                    print("[ -( ] We don't find (", index, ")", row.nom_point_vente)
        except (requests.exceptions.ConnectionError, Exception) as ex:
            traceback.print_exc()
            if isinstance(ex, requests.exceptions.ConnectionError):
                print("Got connexion error, let's sleep for recovered connexion")
                time.sleep(10)
        finally:
            Cdict._to_json(data, "aide_josias.json")





