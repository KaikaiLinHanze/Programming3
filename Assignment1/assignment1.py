from genericpath import exists
from Bio import Entrez
import multiprocessing.dummy as mpd
import multiprocessing as mp
from time import sleep
import sys
import os


def get_reference_list(pubmed_id, db_from="pubmed"):
    """
    :param pubmed_id: str, pubmed id we want to get the article
    :param db_from: str, db
    :return: list of
    """
    Entrez.email = "kaikailin0707@gmail.com"
    key = "93040ced45b4650f365f2c9a9da682381f08"
    record = Entrez.read(Entrez.elink(dbfrom=db_from,
                                      id=pubmed_id,
                                      LinkName="pubmed_pmc_refs",
                                      api_key=key))
    reference_list = []
    for data in record[0]["LinkSetDb"]:
        if data["LinkName"] == "pubmed_pmc_refs":
            reference_data = data["Link"]
            for reference in reference_data:
                reference_list.append(reference["Id"])
    return reference_list


def todo(x):
    """
    :param x: str, reference id
    :return: None
    """
    handle = Entrez.efetch(db="pubmed", id=x, retmode="xml")
    if not os.path.exists("output"):
        os.makedirs("output")
    print("Reaches")
    with open("output/" + x + ".xml", "wb") as xmlfile:
        xmlfile.write(handle.read())
    sleep(10)


if __name__ == "__main__":
    cpus = mp.cpu_count()
    file = str(sys.argv[1])
    ids = get_reference_list(pubmed_id=file)[:10]
    with mpd.Pool(cpus) as pool:
        results = pool.map(todo, ids)
