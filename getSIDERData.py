import requests
import bs4
import sys
import json
import os
import multiprocessing as mp
import time
from pprint import pprint

sys.setrecursionlimit(10000)

BASE_URL = 'http://sideeffects.embl.de/se/'
PAGES_URL = BASE_URL + "?page={}"

# Uncomment if side effect scraping
# JSON_LOC = "Data - SIDER/Side Effects.json"
JSON_LOC = "Data - SIDER/Drugs.json"

MAXPAGE = 166

# Load side effects
# with open("Data - SIDER/Side Effects -Effects.json", "r") as f:
#     sideEffects = json.load(f)

with open(JSON_LOC) as f:
    drugs = json.load(f)


def getSideEffects():

    print("Building Side Effects")
    for _, page in enumerate(range(MAXPAGE + 1)):
        sys.stdout.flush()
        status = round(_ / (MAXPAGE + 1) * 100)
        sys.stdout.write("\rStatus: {}%\r".format(status))

        r = requests.get(PAGES_URL.format(page))
        soup = bs4.BeautifulSoup(r.content, "lxml")
        ul = soup.find("ul", "drugList")

        links = ul.findAll("a")

        # 10:12

        for link in links:
            sideEffects[link.string] = {"link": link["href"]}

        with open(JSON_LOC, "w+") as f:
            json.dump(sideEffects, f)


def getUrl(link, tries=1):
    if tries == 100:
        print("Max retries occurred")
        return ""

    try:
        return requests.get(link, timeout=50)
    except:
        # sleep for a bit in case that helps
        time.sleep(1)
        # try again
        return getUrl(link, tries + 1)

# SE Worker
# def worker(effect, link, q):
#     linkList = []
#     with open("Data - SIDER/Side Effects -Effects.json", 'rb') as f:
#         se = json.load(f)
#
#     r = getUrl(link)
#     soup = bs4.BeautifulSoup(r.content, "lxml")
#     ul = soup.find_all("td")
#
#     for li in ul:
#         linkList.append(li.find_all("a"))
#
#     for links in linkList:
#         for link in links:
#             se[effect][link.string] = link["href"]
#
#     q.put(json.dumps(se))
#
#     return


# Drugs worker
def worker(drug, link, q):
    linkList = []
    with open(JSON_LOC, 'rb') as f:
        drugDict = json.load(f)

    r = getUrl(link)
    soup = bs4.BeautifulSoup(r.content, "lxml")
    div = soup.find("div", {"id": "drugInfoTable"})
    trs = div.find_all("tr")
    tds = {drug: {}}
    for tr in trs:
        td_s = tr.findAll("td", {"class": "nowrap"})
        for _, td in enumerate(td_s):
            if _ == 2:
                break
            else:
                if _ == 0:
                    se = td.find('a').contents[0]
                else:
                    tds[drug][se] = td.contents[0].replace("\n", "")

    drugDict.update(tds)

    q.put(json.dumps(drugDict))

    return


def buildSeJobs(pool, q):
    jobs = []
    tot = len(sideEffects)
    for _, effect in enumerate(sideEffects):
        sys.stdout.flush()
        status = round(_ / (tot) * 100)
        sys.stdout.write("\rStatus: {}%\r".format(status))

        if len(list(sideEffects[effect].keys())) > 1:
            continue

        relLink = sideEffects[effect]["link"]
        link = BASE_URL + relLink + "/pt"

        # fire off worker
        job = pool.apply_async(worker, (effect, link, q))
        jobs.append(job)
    return jobs


def buildDJobs(pool, q):
    jobs = []
    tot = len(drugs)

    for _, drug in enumerate(drugs):
        sys.stdout.flush()
        status = round(_ / (tot) * 100)
        sys.stdout.write("\rStatus: {}%\r".format(status))
        link = "http://sideeffects.embl.de/" + drugs[drug]["link"]
        se = drugs[drug]["se"]

        # fire off worker
        job = pool.apply_async(worker, (drug, link, q))
        jobs.append(job)

    return jobs


def getJobs(jobs):
    tot = len(jobs)
    for _, job in enumerate(jobs):
        sys.stdout.flush()
        status = round(_ / (tot) * 100)
        sys.stdout.write("\rStatus: {}%\r".format(status))
        job.get()


def getDrugs():
    print("Working...")
    manager = mp.Manager()
    q = manager.Queue()
    pool = mp.Pool(12)

    print("building jobs")
    jobs = buildSeJobs()

    print("Getting")
    getJobs(jobs)

    print("Updating Dict")
    while True:
        dic = json.loads(q.get())
        if q.empty():
            break

        for effect in dic:
            if len(dic[effect].keys()) > 1:
                sideEffects[effect].update(dic[effect])
            else:
                continue

    print("Writing Dict")
    with open(JSON_LOC, 'w') as f:
        json.dump(sideEffects, f)

    pool.close()


def getCommonality():
    print("Working...")
    manager = mp.Manager()
    q = manager.Queue()
    pool = mp.Pool(6)

    print("building jobs")
    jobs = buildDJobs(pool, q)

    print("Getting")
    getJobs(jobs)

    print("Updating Dict")
    while True:
        dic = json.loads(q.get())
        if q.empty():
            break

        for drug in dic:
            drugs[drug].update(dic[drug])

    print("Writing Dict")
    with open(JSON_LOC, 'w') as f:
        json.dump(drugs, f)

    pool.close()


if __name__ == "__main__":
    getCommonality()
