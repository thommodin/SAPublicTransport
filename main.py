import pandas as pd
from get_data import get_data, load_pickle

# control vars
REFRESH = True
CSVS = None

def main():

    # download and load all data into a DataFrame
    # df = get_data(refresh=REFRESH, csvs=CSVS, nrows=1_000_000)

    df = load_pickle()
    print(df)
    # filter for bus, train and tram (1, 4 and 5 respectively)
    bus = df[df['NUM_MODE_TRANSPORT'] == 1].reset_index(drop=True)
    train = df[df['NUM_MODE_TRANSPORT'] == 4].reset_index(drop=True)
    tram = df[df['NUM_MODE_TRANSPORT'] == 5].reset_index(drop=True)

    print(train.head(50))

if __name__ == '__main__':
    main()
