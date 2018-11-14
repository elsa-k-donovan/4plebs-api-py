#!/usr/bin/env python
import json
import requests
import time
import pandas as pd
import datetime
import calendar

from elasticsearch import Elasticsearch

SEARCH_PARAMS = {
    # "start_date": "2016-05-05",
    # "end_date": "2019-05-07",
    "boards": ["pol"],
    "page_limit": 2,
    "requests_per_min": 5,
    "index": "dataframe",
    "type": "record"
}

DATE_PARAMS = {
    "year": 2016,
    "month": 1
}


class Pleb:
    def __init__(self, start_date, end_date, boards, index, type, page_limit=float('inf'), requests_per_min=5):
        self.rate_limit = 60 / requests_per_min
        self.boards = ".".join(boards)
        self.page_limit = page_limit
        self.start = start_date
        self.end = end_date
        self.current_page = 1
        self.index = index
        self.type = type
        self.base_url = "http://archive.4plebs.org/_/api/chan/search/" + "?boards=" + \
            self.boards + "&start=" + start_date + "&end=" + end_date + "&page="

    def _download_page(self):
        """ Downloads a 4plebs archive page into a pandas normalized format. """

        result = requests.get(self.base_url + str(self.current_page))
        result.raise_for_status()
        result = json.loads(result.text)["0"]["posts"]
        return pd.io.json.json_normalize(result)

    def _rec_to_actions(self, df):
        """ Yields the documents to bulk-post to ES's bulk API given a pandas DF """

        for record in df.to_dict(orient="records"):
            yield ('{ "index" : { "_index" : "%s", "_type" : "%s" }}' % (self.index, self.type))
            yield (json.dumps(record, default=int))

    def _store_es(self, acc):
        """ Stores the dataframe in an ES node (default localhost:9200) """

        e = Elasticsearch()  # no args, connect to localhost:9200
        if not e.indices.exists(self.index):
            request_body = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }
            print("creating '%s' index..." % (self.index))
            e.indices.create(index=self.index, body=request_body)

        r = e.bulk(self._rec_to_actions(acc))  # return a dict
        if not r["errors"]:
            print(str(len(acc)) + " documents posted to ElasticSearch")
        else:
            print(" !!! ERROR !!! ", r)

    def save_data(self, fnm=None, es_store=False):
        """ Downloads your specified query into either a csv file or an active ES node.
            By default csv's filename is {start date}_{end date}_{b,o,a,r,d,s}.csv
            ES node is expected on localhost:9200
        """

        if fnm is None:
            fnm = "_".join([self.start, self.end, self.boards]) + ".csv"

        acc = pd.DataFrame()
        while(self.current_page < self.page_limit):
            try:
                results = self._download_page()
                self.current_page += 1
            except Exception as e:
                print(e)
                break

            acc = pd.concat([acc, results], ignore_index=True).fillna('No Info')
            del acc["media"]
            time.sleep(self.rate_limit)
        # Branch treatment according to es_store
        if not es_store:
            acc.to_csv(fnm)
        else:
            self._store_es(acc)
        return acc


def days_from_month(year, month):
    """ Given a year and a month (2014,5) returns the str() of its days
        in a 4plebs query-friendly format (YYYY-MM-DD)
    """
    return [datetime.date(year, month, day) for day in range(1, calendar.monthrange(year, month)[1] + 1)]


def scrape_month_into_es(year, month):
    """ Samples {SEARCH_PARAMS["page_limit"]}'s first pages per day of the given month.
        Since we're just brute-forcing and stopping to look after this is the fastest way
        of sampling we currently have.
    """
    starting_dates = days_from_month(year, month)

    for start_date in starting_dates:
        try:
            # A fancy new way to say tomorrow
            SEARCH_PARAMS["start_date"] = str(start_date)
            SEARCH_PARAMS["end_date"] = str(start_date + datetime.timedelta(1))

            pb = Pleb(**SEARCH_PARAMS)
            pb.save_data(es_store=True)
            time.sleep(60 / SEARCH_PARAMS["requests_per_min"])
        except Exception as e:
            print(e)


if __name__ == '__main__':
    scrape_month_into_es(**DATE_PARAMS)
