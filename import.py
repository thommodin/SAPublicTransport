import pandas as pd
import numpy as np
import os, sys


def download(urls, refresh=False):

    # requirements
    import requests, zipfile, io, os

    # we can override re-downloading to the data folder if we want
    if not refresh:
        return

    # download and extract the urls
    for url in urls:

        # get zip
        r = requests.get(url)

        # encode as zipfile
        z = zipfile.ZipFile(io.BytesIO(r.content))

        # extract
        z.extractall('data/')

def load_data(csvs=None):

    # construct paths for data files if not supplied, platform independent
    if not csvs:
        csvs = [os.path.join('data', csv) for csv in os.listdir('data')]

    # attempt to remove any hidden files
    csvs = list(filter(lambda csv: csv[0] != '.', csvs))

    # remove too short candidate csv files
    csvs = list(filter(lambda csv: len(csv) > 4, csvs))

    # remove non csv file suffixes
    csvs = list(filter(lambda csv: csv[-4:] == '.csv', csvs))

    # if there is no data exit early
    if len(csvs) < 1: sys.exit('No data in data folder!')

    # platform independent read all csv in 'data' folder
    return pd.concat([pd.read_csv(csv) for csv in csvs])

def main():

    # list of data urls
    urls = [
    'https://data.sa.gov.au/data/dataset/5b55dce0-6f75-4702-a9a5-f36413e0a27c/resource/f65322ce-8426-4403-b9c2-205148534a58/download/bandedvalidations2020-01-02-03.zip', # 2020-Q1
    'https://data.sa.gov.au/data/dataset/5b55dce0-6f75-4702-a9a5-f36413e0a27c/resource/7f774648-66a1-404c-b296-48342226d7a8/download/bandedvalidations2020-04-05-06.zip', # 2020-Q2
    'https://data.sa.gov.au/data/dataset/5b55dce0-6f75-4702-a9a5-f36413e0a27c/resource/9383fae9-c429-4d1b-9729-a736c2865bfd/download/bandedvalidations2019-10-11-12.zip', # 2019-Q4
    'https://data.sa.gov.au/data/dataset/5b55dce0-6f75-4702-a9a5-f36413e0a27c/resource/b6a20b09-71fa-4c3b-9ba9-dfab979eb021/download/bandedvalidations2019-07-08-09.zip', # 2019-Q3
    'https://data.sa.gov.au/data/dataset/5b55dce0-6f75-4702-a9a5-f36413e0a27c/resource/799c4402-7bf8-408c-96ea-29b0fae596bb/download/bandedvalidations2019-04-05-06.zip', # 2019-Q2
    'https://data.sa.gov.au/data/dataset/5b55dce0-6f75-4702-a9a5-f36413e0a27c/resource/0ce28d97-7e37-4b9a-b6ef-379b2323fdff/download/bandedvalidations2019-01-02-03.zip', # 2019-Q1
    ]

    # download all the data
    download(urls, refresh=True)

    # construct list of data directories
    df = load_data()

    print(df)

if __name__ == '__main__':
    main()
