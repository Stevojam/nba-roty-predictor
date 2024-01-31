import pandas as pd
import time
from tqdm import tqdm
import bref_scraper as bref
import os

def historical_roy_awards(start_year, end_year):
    """
    Gathers rookie of the year awards data from start year to end year
    """
    years = [year for year in range(start_year, end_year)]
    table = []

    for year in tqdm(years):
        print(f"Extracting ROY data for {year}")
        roy_awards = bref.extract_roy_awards(year)
        if roy_awards is not None:
            roy_awards['year'] = year
            table.append(roy_awards)

        time.sleep(3) #Adding 3 seconds to avoid being blocked 

    roy = pd.concat(table)

    return roy

def historical_advanced_stats(start_year, end_year):
    """
    Gathers advanced statistics data from start year to end year
    """
    years = [year for year in range(start_year, end_year)]
    table = []

    for year in tqdm(years):
        print(f"Extracting advanced statistics data for {year}")
        advanced_stats_table= bref.extract_advanced_stats(year)
        if advanced_stats_table is not None:
            advanced_stats_table['year'] = year
            table.append(advanced_stats_table)
        
        time.sleep(3) #Adding 3 seconds to avoid being blocked 

    advanced_stats = pd.concat(table)

    return advanced_stats

def historical_standings(start_year, end_year):
    """
    Gathers advanced statistics data from start year to end year
    """
    years = [year for year in range(start_year, end_year)]
    table = []

    for year in tqdm(years):
        print(f"Extracting standings for {year}")
        standings_table = bref.extract_standings(year)
        if standings_table is not None:
            standings_table['year'] = year
            table.append(standings_table)
        
        time.sleep(3) #Adding 3 seconds to avoid being blocked 

    standings = pd.concat(table)

    return standings


start_year = 1979
end_year = 2024
cur_dir = os.getcwd()

### Extract ROY table
roy_table = historical_roy_awards(start_year, end_year)
cur_dir = os.getcwd()
roy_table.to_csv(cur_dir + '/data/history_roy.csv', index = False)

### Extract Advanced stats
history_advanced_stats = historical_advanced_stats(start_year, end_year)
history_advanced_stats.to_csv(cur_dir + '/data/history_advanced_stats.csv', index = False)

### Extract standings
history_standings = historical_standings(start_year, end_year)
history_standings.to_csv(cur_dir + '/data/history_standings.csv', index = False)

advanced_stats = pd.read_csv(cur_dir+'/data/history_advanced_stats.csv')
roy_table = pd.read_csv(cur_dir+'/data/history_roy.csv')
standings_table = pd.read_csv(cur_dir+'/data/history_standings.csv')


#Merge dataframes and remove duplicate columns
table = pd.merge(roy_table, advanced_stats, how='left', on=['Player', 'year', 'Tm'], suffixes=('', '_remove'))
table.drop([column for column in table.columns if 'remove' in column], axis=1, inplace=True)

#Merge team standings to the table
table = pd.merge(table, standings_table, how='left', left_on=['Tm', 'year'], right_on=['TEAM_ABBREVIATION', 'year'], suffixes=('', '_remove'))
table.drop([column for column in table.columns if 'remove' in column], axis=1, inplace=True)

to_drop = [
    'Age',
    'First', 
    'Pts Won', 
    'Pts Max',
    'Rk',
    'TEAM_NAME',
    'Conference', 
    'TEAM_ABBREVIATION',
    'GB',
    'PS/G', 
    'PA/G', 
    'SRS'
]

master_table = table.drop(columns=to_drop)

### Create final master_table and store it in a csv
master_table_copy = master_table.copy()
master_table_copy['W'] = master_table_copy['W'].fillna(round(master_table_copy['W'].mean()))
master_table_copy['L'] = master_table_copy['L'].fillna(round(master_table_copy['L'].mean()))
master_table_copy['W/L%'] = master_table_copy['W/L%'].fillna(master_table_copy['W/L%'].mean())
master_table_copy[['3P%', '3PAr']] = master_table_copy[['3P%', '3PAr']].fillna(0)

master_table_copy.to_csv(cur_dir + '/data/master_table.csv', index = False)