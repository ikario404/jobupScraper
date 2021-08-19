import time
import requests
from bs4 import BeautifulSoup
# from pandas import DataFrame, read_csv
from furl import furl
import sqlite3
from datetime import date


class sqlCRUD:
    # SQLClass to match our needs
    def __init__(self, cnt):
        try:
            self.cnt = sqlite3.connect(sql_filename)
        except sqlite3.Error as err:
            print(err)
            return err

    def create_if_not_exist(self):
        # table_name = "jobdb"
        try:
            self.cnt.execute('''CREATE TABLE IF NOT EXISTS jobdb (
            title,
            company,
            location,
            url,
            jobup_id TEXT NOT NULL UNIQUE,
            date_due,
            date_gathered,
            content TEXT)''')
            # self.cnt.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx ON jobdb (jobup_id)''')
            self.cnt.commit()
        except sqlite3.Error as err:
            print(err)
            # logger.error(err.message)
            return err

    def read_sql(self):
        try:
            result = self.cnt.execute('''SELECT * FROM jobdb;''')
            self.cnt.commit()
            # self.cnt.close()
        except sqlite3.Error as err:
            print(err)
            result = err
        return result

    def read_one_sql(self, id_job):
        try:
            result = self.cnt.execute(
                '''SELECT * FROM jobdb WHERE jobdb.jobup_id = (?);''', (id_job,))
            self.cnt.commit()
        except sqlite3.Error as err:
            print(err)
            result = err
        return result

    def insert_sql(self, item_data):
        sql_query = '''
                INSERT INTO jobdb (title, company, location, url, jobup_id, date_due, date_gathered)
                VALUES (?, ?, ?, ?, ?, ?, ?); '''
        try:
            self.cnt.execute(
                sql_query, (item_data['title'], item_data['company'], item_data['location'], item_data['url'], item_data["jobup_id"], item_data['date_due'], item_data['date_gathered']))
            self.cnt.commit()
            self.cnt.close()
        except sqlite3.Error as err:
            print(err)
            self.cnt.close()
            return err

    def insert_content(self, item_data):
        sql_query = '''UPDATE jobdb SET content=? WHERE jobup_id=?;'''
        try:
            self.cnt.execute(
                sql_query, item_data)
            self.cnt.commit()
            self.cnt.close()
        except sqlite3.Error as err:
            print(err)
            return err

    def close_sql(self):
        self.cnt.close()


def scrap_jobup(main_url):
    # prepare our object
    offers = []

    # Request the page & parse
    jobup = requests.get(main_url)
    soup = BeautifulSoup(jobup.text, "lxml")

    # catch next iterations url from previous soup
    next_link = soup.find("a",
                          {"data-cy": "paginator-next",
                              "class": "sc-hGPAah Link-sc-1vy3ms6-1 dAcVeS"},
                          href=True).attrs["href"]
    next_url_request = requests.get(host + next_link)
    soup = BeautifulSoup(next_url_request.text, "lxml")

    i = 0
    # loop to paginate
    while next_url_request:
        print("scrap_first() == ",
              "We are now to result number page from jobup.ch/ --> ", i)
        i += 1
        # if i == 2:
        #    break
        # catch div of jobs
        job_info = soup.find_all(
            "div", {"class": "sc-dkPtyc VacancySerpItem__ShadowBox-y84gv4-0 ihIdXD dfsFuI"})

        # iterate on each one to catch data
        for x in job_info:
            # very rough and barbare but need to check if each elements exists...
            # title
            # eXzNTw part is moving...
            if x.find("span", {"class": "sc-fotPbf Text__span-jiiyzm-8 Text-jiiyzm-9 eXzNTw bGEEwS"}):
                title = x.find("span", {
                               "class": "sc-fotPbf Text__span-jiiyzm-8 Text-jiiyzm-9 eXzNTw bGEEwS"}).text
            else:
                title = "NA"
            # company
            # ljqydn is moving
            if x.find("span", {"class": "sc-fotPbf Text__span-jiiyzm-8 Text-jiiyzm-9 ljqydn eQChBQ"}):
                company = x.find("span", {
                                 "class": "sc-fotPbf Text__span-jiiyzm-8 Text-jiiyzm-9 ljqydn eQChBQ"}).text
            elif x.find("span", {"class": "sc-fotPbf Text__span-jiiyzm-8 Text-jiiyzm-9 fJSCvO eQChBQ"}):
                company = x.find("span", {
                                 "class": "sc-fotPbf Text__span-jiiyzm-8 Text-jiiyzm-9 fJSCvO eQChBQ"}).text
            else:
                company = "NA"
            # location
            # dXyOot  is moving
            if x.find("span", {"class": "sc-fotPbf Text__span-jiiyzm-8 Text-jiiyzm-9 dXyOot eQChBQ"}):
                location = x.find("span", {
                                  "class": "sc-fotPbf Text__span-jiiyzm-8 Text-jiiyzm-9 dXyOot eQChBQ"}).text
            else:
                location = "NA"

            # url
            # cGeykq  is moving
            if x.find("a", {"data-cy": "job-link", "class": "sc-hGPAah cGeykq Link-sc-1vy3ms6-1 dAcVeS"}, href=True):
                url = x.find("a", {
                             "data-cy": "job-link", "class": "sc-hGPAah cGeykq Link-sc-1vy3ms6-1 dAcVeS"}, href=True).attrs["href"]
            else:
                url = "NA"

            # date_due
            if x.find("span", {"class": "sc-fotPbf Text__span-jiiyzm-8 Text-jiiyzm-9 ljqydn eQChBQ"}):
                date_due = x.find("span", {
                                  "class": "sc-fotPbf Text__span-jiiyzm-8 Text-jiiyzm-9 VacancySerpItem___StyledText-y84gv4-6 jaHdBs eQChBQ cxIvvs"}).text
            else:
                date_due = "NA"

            # get jobup id paged by url
            jobup_id = furl(url)
            del jobup_id.args["source"]
            jobup_id = jobup_id.pathstr[19:].replace("/", "")

            # make query to db if it already exist.
            # if so pass to next jobid
            if sqlCRUD(cnt).read_one_sql(jobup_id).fetchone():
                print("scrap_first() == ", jobup_id, "is already in db")
                continue

            offer = {
                "title": title,
                "company": company,
                "location": location,
                "url": host + url,
                "jobup_id": jobup_id,
                "date_due": date_due,
                "date_gathered": today.strftime("%d.%m.%Y"),
            }

            # save SQL
            sqlCRUD(cnt).insert_sql(offer)

            # append to obj
            offers.append(offer)

        if soup.find("a", {"data-cy": "paginator-next", "class": "sc-hGPAah Link-sc-1vy3ms6-1 dAcVeS"}):
            next_link = soup.find("a", {"data-cy": "paginator-next", "class": "sc-hGPAah Link-sc-1vy3ms6-1 dAcVeS"},
                                  href=True).attrs["href"]
            next_url_request = requests.get(host + next_link)
            soup = BeautifulSoup(next_url_request.text, "lxml")
        else:
            next_url_request = None
        print("scrap_first() == ", "Number of total job offer is --> ", len(offers))
    return offers


def get_data_offer(uri):
    url = host + uri
    url = uri
    offer_data = requests.get(url)
    time.sleep(0.3)
    soup = BeautifulSoup(offer_data.text, "lxml")
    print(url)
    if not (soup.find("iframe", {"data-cy": "detail-vacancy-iframe-content"}) is None):
        data = BeautifulSoup(soup.find(
            "iframe", {"data-cy": "detail-vacancy-iframe-content"}).attrs['srcdoc'], "lxml").text
    elif soup.find("div", {"class": "sc-dkPtyc bGxgLd"}):
        data = soup.find("div", {"class": "sc-dkPtyc bGxgLd"}).text
    else:
        data = "NA"
    return data


# def save_data(namefile, data):
#     DataFrame(data).to_csv(namefile, encoding="UTF-8")
#     return


def loop_on_each_offer(offers_data):
    i = 0
    for offer in offers_data:
        i += 1
        offer_content = get_data_offer(offer['url'])
        item_data = (offer_content, offer["jobup_id"])
        sqlCRUD(cnt).insert_content(item_data)
        time.sleep(0.4)
        if i % 10 == 0:
            print("loop_each() == ", i, "--> items are already done")

    return offers_data


# Path of file db
path = 'SET/here/your/path'
sql_filename = path + "./name-your-file.db"

# host, date, url and segment
host = "https://www.jobup.ch"
today = date.today()

# example request url
main_url = "https://www.jobup.ch/fr/emplois/?location=Fribourg&region=52&region=57&region=42&region=40&region=34&region=30&term=ingenieur"
f = furl(main_url)
segment_url = f.path.segments[-2]
segment_url = (f.querystr.replace("=", "")).replace("&", "_")


# Those main functions will :
# 1. first check each url and paginate till nothing while getting maximum amount of data
# note it will try to get all url at first. Limite it with iteration number of while (sorted by date from jobup.ch)
# 2. Second request earch url of job offer to get content of offer
# From now a someway SQLCRUD is integrated inside main functions

if __name__ == '__main__':
    print("main.py == ", segment_url)

    # init DB
    cnt = sqlite3.connect(sql_filename)
    sqlite_db = sqlCRUD(cnt)
    sqlite_db.create_if_not_exist()

    # start from webapage
    offers_data = scrap_jobup(main_url)
    print("main.py == first save job done ==")

    # loop on following pages...
    offers_data_enhanced = loop_on_each_offer(offers_data)
