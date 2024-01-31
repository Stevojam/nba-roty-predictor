import bref_scraper as bs
import pandas as pd
import tqdm

pd.set_option('display.max_columns', None)

def create_historical_table(start_year, end_year):
    """
    Create the historical master data table
    """
    years = [year for year in range(start_year, end_year)]
    tables = []

    for year in years:
        roy_votes = bs.extract_roy_awards(year)
        advanced_stats = bs.extract_advanced_stats(year)
        standings = bs.extract_team_standings(year)

        #Merge dataframes and remove duplicate columns
        table = pd.merge(roy_votes, advanced_stats, how='left', on='Player', suffixes=('', '_remove'))
        table.drop([column for column in table.columns if 'remove' in column], axis=1, inplace=True)


        #Merge team standings to the table
        table = pd.merge(table, standings, how='left', on='Tm', suffixes=('', '_remove'))
        table.drop([column for column in table.columns if 'remove' in column], axis=1, inplace=True)

        #Add year to the table
        table['Year'] = year

        tables.append(table)

    master_table = pd.concat(tables)

    return master_table

print(create_historical_table(1979, 2023))
    

    



