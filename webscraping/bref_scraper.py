import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm import tqdm

pd.set_option('display.max_columns', None)

headers = {
    'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36"
    }

def extract_rookies_data(year):
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_rookies-season-stats.html"

    try:
        pageTree = requests.get(url, headers=headers)

        pageTree.raise_for_status()

        soup = BeautifulSoup(pageTree.content, 'html.parser')
        table = soup.find("table", id="rookies")

        if table is not None:
            rookies = pd.read_html(str(table), header=1)[0]

            rookies = rookies.rename(columns={"MP.1": "MP/G", 'PTS.1': "PTS/G", "TRB.1": "TRB/G", "AST.1": "AST/G"})

            # Remove mid-table header rows
            rookies = rookies[rookies['Rk'].notna()]
            rookies = rookies[~rookies['Rk'].str.contains('Rk')]

            return rookies
        
        else:
            print(f"No 'rookies' table found for the year {year}")
            return pd.DataFrame()  

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return None  
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None  # or handle the general exception as needed


def extract_roy_awards(year):
    """
    Extracts rookie of the year votes and statistics from basketball-reference for a specific year
    """
    
    url = f"https://www.basketball-reference.com/awards/awards_{year}.html"

    try:
        pageTree = requests.get(url, headers=headers)

        pageTree.raise_for_status()

        soup = BeautifulSoup(pageTree.content, 'html.parser')
        table = soup.find("table", id="roy")

        if table is not None:
            roy_voting = pd.read_html(str(table), header=1)[0]
            return roy_voting
        
        else:
            print(f"No 'roy' table found for the year {year}")
            return pd.DataFrame()  

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return None  
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None  # or handle the general exception as needed



def extract_advanced_stats(year):
    """
    Extracts advanced stats from basketball-reference for the year
    """
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_advanced.html"

    try:
        pageTree = requests.get(url, headers=headers)

        pageTree.raise_for_status()

        soup = BeautifulSoup(pageTree.content, 'html.parser')
        table = soup.find("table", id="advanced_stats")

        if table is not None:
            advanced_stats = pd.read_html(str(table))[0]

            #Drop unwanted columns
            advanced_stats.drop(['Unnamed: 19', 'Unnamed: 24'], axis=1, inplace=True)
            advanced_stats = advanced_stats[~advanced_stats['Rk'].str.contains('Rk')]

            #Remove the star for players in the Hall of Fame
            advanced_stats['Player'] = advanced_stats['Player'].str.replace('*', '', regex=False)

            return advanced_stats

        else:
            print(f"No 'advanced_stats' table found for the year {year}")
            return pd.DataFrame()
                  
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return None  
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None  # or handle the general exception as needed 


def extract_standings(year):
    """
    Extracts the NBA standings from basketball-reference for a specific year
    """
    
    try:
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html"

        pageTree = requests.get(url, headers=headers)

        pageTree.raise_for_status()
        
        soup = BeautifulSoup(pageTree.content, 'html.parser')

        table_east = soup.find("table", id="divs_standings_E")
        table_west = soup.find("table", id="divs_standings_W")

        if table_east is not None and table_west is not None:
            east_standings = pd.read_html(str(table_east))[0]
            east_standings['Conference'] = "East"
            east_standings.rename(columns={'Eastern Conference': 'TEAM_NAME'}, inplace=True)
        
            west_standings = pd.read_html(str(table_west))[0]
            west_standings.rename(columns={'Western Conference': 'TEAM_NAME'}, inplace=True)
            west_standings['Conference'] = "West"

        else:
            print(f"No 'standings' table found for the year {year}")
            return pd.DataFrame()
        
        standings = pd.concat([east_standings, west_standings], axis=0).reset_index(drop=True)
        standings['TEAM_NAME'] = standings['TEAM_NAME'].str.replace('*', '', regex=False)

        ## Get team abbreviation from the a tags in the page
        a_tags = soup.select('table.stats_table a')
        
        if a_tags is not None:
            team_abbreviation = [a['href'].split('/')[2] for a in a_tags]
            team_names = [a.text for a in a_tags]
            df_teams = pd.DataFrame({'TEAM_ABBREVIATION': team_abbreviation, 'TEAM_NAME': team_names})
            df_teams.drop_duplicates(inplace=True)

            standings = pd.merge(standings, df_teams, how='inner', on="TEAM_NAME")

            return standings

        else:
            print(f"No 'a_tags' found in page for the year {year}")
            return standings

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return None  
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None  # or handle the general exception as needed




