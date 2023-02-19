import pandas

from kb_package.utils.fdataset import DatasetFactory
from kb_package.tools import CModality, Cdict, read_json_file
import re
import copy

NUMBER_VERIFY_REG = r"^\(?(?:00|\+)?(?:225\)?)?(\d{1,2})?(\d{8})$"
numerotation = {
    "05": ["04", "05", "06", "44", "45", "46", "54", "55", "56",
           "64", "65", "66", "74", "75", "76", "84", "85", "86",
           "94", "95", "96"],
    "01": ["01", "02", "03", "40", "41", "42", "43", "50",
           "51", "52", "53", "70", "71", "72", "73"],

    "07": ["07", "08", "09", "47", "48", "49", "57", "58", "59",
           "67", "68", "69", "77", "78", "79", "87", "88", "89",
           "97", "98"],
}


def check_number(number, plus="+", only_orange=False, permit_fixe=True):
    number = str(number).replace(" ", "").replace("-", "")
    check = re.match(NUMBER_VERIFY_REG, str(number).split(".")[0].split(",")[0])
    nums = copy.deepcopy(numerotation)
    if check:
        check = check.groups()

        extension = check[0]
        if extension is None:
            # numero avec 8 chiffre
            extension = check[1][:2]
            # ancienne numérotation
            if int(extension) in list(range(20, 25)) + list(range(30, 37)):
                # Fixe
                if not permit_fixe:
                    return None
                if check[1][2] == "8" and not only_orange:
                    # MOOV - 21
                    return plus + "22521" + check[1]
                elif check[1][2] == "0" and not only_orange:
                    # MTN - 25
                    return plus + "22525" + check[1]
                else:
                    # ORANGE - 27
                    return plus + "22527" + check[1]
            else:
                # mobile
                if only_orange:
                    nums.pop("01")
                    nums.pop("05")
                for num in nums:
                    if extension in nums.get(num):
                        return plus + "225" + num + check[1]
        elif extension in ("7,07,27"+("" if only_orange else ",21,25,1,01,5,05")).split(","):
            return plus + "225" + f"{extension:0>2}" + check[1]
    return None


def parse_address(address):
    address = address["Adresse"]
    if address is None:
        return address
    address = address.encode("utf-8").decode('utf-8-sig')
    address = " ".join([p for p in address.split() if len(p)])
    address = re.split(r"(\d{1,2} BP \d+ (?:ABIDJAN|[a-z]+) \d{1,2})", address, flags=re.I | re.S)
    bp = None
    if len(address) > 2:
        bp = address.pop(1)
        # print(" ".join(address).strip(), address)
    address = " ".join(address).strip()
    address = address if not address.startswith("-") else address[1:].strip()
    # address = re.split("[A-Z]", address, flags=re.S)
    return pandas.Series([address, bp], index=["Adresse", "Boite Postale"])


def processing(i=None):
    if i is None:
        file = r"C:\Users\FBYZ6263\Downloads\josias\company.csv"
        merge_file = "aide_josias.json"
        export = "result_company.xlsx"
    else:
        file = r"josias_base_" + str(i) + ".csv"
        merge_file = r"josias_base_" + str(i) + "_export.json"
        export = "result_company_" + str(i) + ".xlsx"

    company = DatasetFactory(file).dataset
    company.rename(columns={"nom_point_vente": "Raison sociale", "sigle_point_vente": "Sigle"}, inplace=True)
    if 'CONTACTS / EMAILS' in company.columns:
        company.rename(columns={'CONTACTS / EMAILS': "Contacts"}, inplace=True)
    print(company.columns)
    data = DatasetFactory(read_json_file(merge_file, [])).dataset

    res = company.merge(data, left_index=True, right_on="index", sort=False)
    del res["index"]

    # res["Contacts"] = res.Contacts.apply(check_number)

    res["Contacts"] = res.Contacts.apply(
        lambda nb: " / ".join([str(d) for d in [check_number(x) for x in str(nb).split("/")] if d is not None]))
    print(res.Contacts)
    if "Adresse" in res.columns:
        address = res.apply(parse_address, axis=1)
        # res.loc[:, ["Adresse Postale", "Adressef"]] = res.apply(parse_address, axis=1, result_type='expand')
        # print(res["Adresse Postale"])
        res.loc[:, ["Adresse", "Boite Postale"]] = address
        if i == 1:
            del res["Adresse"]
    print(res)
    res.to_excel(export)


def draft():
    for i in [1, 2]:
        file = rf"C:\Users\FBYZ6263\Documents\OWN\kb_package\temp\{i}.xlsx"

        company = DatasetFactory(file).dataset
        company.rename(columns={"nom_point_vente": "Raison sociale", "sigle_point_vente": "Sigle"}, inplace=True)
        res = read_json_file(rf"C:\Users\FBYZ6263\Documents\OWN\kb_package\temp\josias_base_{i}_export.json", [])
        res = [d for d in res if isinstance(d, dict)]
        data = DatasetFactory(res).dataset

        res = company.merge(data, left_index=True, right_on="index", sort=False)
        del res["index"]
        res["Contacts"] = res.Contacts.apply(
            lambda nb: " / ".join([str(d) for d in [check_number(x) for x in str(nb).split("/")] if d is not None]))
        print(res)
        print(res.Contacts)
        res.to_excel(f"result_company - Brouillon Entreprises_{i}.xlsx")


if __name__ == '__main__':
    draft()
