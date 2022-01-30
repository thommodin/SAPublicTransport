import pandas as pd
from tqdm import trange
import os, sys

def download(refresh=False):

    # requirements
    import requests, zipfile, io, os

    # we can override re-downloading to the data folder if we want
    if not refresh:
        return

    print('Scraping for all downloads. . .')

    # scrape thge website for all available files
    from lxml import html
    import requests

    # get the page
    page = requests.get('https://data.sa.gov.au/data/dataset/adelaide-metrocard-validations')

    # extract into tree
    tree = html.fromstring(page.content)

    # get the urls
    urls = tree.xpath('//a[@target="_blank"]/@href')

    # extract only urls that are datasets
    urls = list(filter(lambda url: url[:36] == 'https://data.sa.gov.au/data/dataset/', urls))

    # extract only the links that have string '/download/bandedvalidations20' in them
    urls = list(filter(lambda url: '/download/bandedvalidations20' in url, urls))

    # download and extract the urls
    print('Downloading files from found download urls!')
    for i in trange(len(urls)):

        # get zip
        r = requests.get(urls[i])

        # if the url gets a .zip, extract the file
        if urls[i][-4:] == '.zip':

            # encode as zipfile
            z = zipfile.ZipFile(io.BytesIO(r.content))

            # extract
            z.extractall('data')

        # if the url gets a .csv, save it
        elif urls[i][-4:] == '.csv':

            # write with the last 34 characters as the file name
            with open(os.path.join('data', urls[i][-34:]), 'w') as f:
                f.write(r.text)

def load(csvs=None, nrows=None):

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

    print('Loading all data into DataFrame. . .')
    # platform independent read all csv in 'data' folder
    df = pd.concat([pd.read_csv(csv, nrows=nrows) for csv in csvs])

    # reorder by date, newest to oldest
    df['VALIDATION_DATE'] = pd.to_datetime(df['VALIDATION_DATE'])
    df.sort_values(ascending=False, by='VALIDATION_DATE', inplace=True)

    return df.reset_index(drop=True)

def get_data(refresh=False, csvs=None, nrows=None):

    download(refresh=refresh)

    return load(csvs=csvs, nrows=nrows)

def save_feather():

    download(refresh=True)

    print('Saving to .feather in data folder')
    load().to_feather(os.path.join('data', 'data.feather'))

def load_feather():

    # get and save a feather file if it does not exist
    if not 'data.feather' in os.listdir('data'): save_feather()

    # return df from feather file
    return pd.read_feather(os.path.join('data', 'data.feather'))
