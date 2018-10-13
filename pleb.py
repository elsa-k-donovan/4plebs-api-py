import json
import requests
import time
import pandas as pd

SEARCH_PARAMS = {
    "start_date" : "2017-05-05",
    "end_date" : "2017-05-07",
    "boards" : ["pol"],
    "page_limit" : 2
}

class Pleb:
    def __init__(self, start_date, end_date, boards, page_limit = float('inf'), requests_per_min=5):
        self.rate_limit = 60/requests_per_min
        self.boards = ".".join(boards)
        self.page_limit = page_limit
        self.start = start_date
        self.end = end_date
        self.current_page = 1
        self.base_url = "http://archive.4plebs.org/_/api/chan/search/" + "?boards="+ self.boards + "&start="+ start_date + "&end="+ end_date +"&page="
        
    def download_page(self):
        result = requests.get(self.base_url+ str(self.current_page))
        result.raise_for_status()
        result = json.loads(result.text)["0"]["posts"]
        return pd.io.json.json_normalize(result)
            
    def save_data(self, fnm=None):
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
        acc.to_csv(fnm)
        
pb = Pleb(**SEARCH_PARAMS)
