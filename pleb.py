import json
import requests
import time
import pandas as pd
import json

from elasticsearch import Elasticsearch

SEARCH_PARAMS = {
    "start_date" : "2017-05-05",
    "end_date" : "2017-05-07",
    "boards" : ["pol"],
    "page_limit" : 2,
    "index" : "dataframe",
    "type" : "record"
}

class Pleb:
    def __init__(self, start_date, end_date, boards, page_limit = float('inf'), requests_per_min=5, index="dataframe", type="record"):
        self.rate_limit = 60/requests_per_min
        self.boards = ".".join(boards)
        self.page_limit = page_limit
        self.start = start_date
        self.end = end_date
        self.current_page = 1
        self.index = index
        self.type = type
        self.base_url = "http://archive.4plebs.org/_/api/chan/search/" + "?boards="+ self.boards + "&start="+ start_date + "&end="+ end_date +"&page="
        
    def download_page(self):
        result = requests.get(self.base_url+ str(self.current_page))
        result.raise_for_status()
        result = json.loads(result.text)["0"]["posts"]
        return pd.io.json.json_normalize(result)
            
    def save_data(self, fnm=None, es_store=False):
        if fnm is None : 
            fnm = "_".join([self.start,self.end,self.boards])+".csv"
        acc = pd.DataFrame()
        while(self.current_page < self.page_limit):
            try:
                results = self.download_page()
                self.current_page += 1
            except Exception as e:
                print(e)
                break
            
            acc = pd.concat([acc, results], ignore_index=True)
            time.sleep(self.rate_limit)
        if not es_store:
            acc.to_csv(fnm)
        else:
            e = Elasticsearch() # no args, connect to localhost:9200
            if not e.indices.exists(self.index):
                raise RuntimeError('index does not exists, use `curl -X PUT "localhost:9200/%s"` and try again'%self.index)

            r = e.bulk(self.rec_to_actions(acc)) # return a dict

    def rec_to_actions(self, df):

        for record in df.to_dict(orient="records"):
            yield ('{ "index" : { "_index" : "%s", "_type" : "%s" }}'% (self.index, self.type))
            yield (json.dumps(record, default=int))
        
pb = Pleb(**SEARCH_PARAMS)
# pb.save_data()
