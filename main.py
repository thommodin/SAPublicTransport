import pandas as pd
from get_data import get_data

# control vars
REFRESH = False
CSVS = None

def main():

    # download and load all data into a DataFrame
    df = get_data(refresh=REFRESH, csvs=CSVS, nrows=10_000)

    

    print(df)

if __name__ == '__main__':
    main()
