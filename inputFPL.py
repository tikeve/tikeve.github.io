''' FPL Download
    
    Downloads data from https://fantasy.premierleague.com/ and calculates basic pandas tables.
    Depending on sys.argv[1] 'full', 'medium' or anything else does full download (Optional module,
    medium download or nothing.
    
    Uses module: Optional

    Sources:    FPL API
                'in/bootstrap.csv'
                'in/Fixtures.csv'
                'in/LTable_FPL.csv'
                'in/Team_fixtures.csv'
                'in/Player_fixtures.csv'
                
    Writes:     'in/Fixtures.csv'
                'in/Table_FPL.csv'
                'in/Teams.csv'
                'in/Players.csv'
                'in/Team_played_fixtures.csv'
                'in/Team_upcoming_fixtures.csv'
                'in/bootstrap.csv'
                'in/LTable_FPL.csv'
                'in/Player_played_fixtures.txt'
                'in/Player_upcoming_fixtures.txt'

'''

#Downloadting Data from fantasy.premierleague.com(FPL)a
print('Start inputFPL:')
from time import time
start_module = time()
start = start_module

from constti import strip_accents_pdlist, long_request, differences
from Brr_functions import to_lists, no_lists, del_empty_col
from bs4 import BeautifulSoup
import pandas as pd
import json
import numpy as np
from pathlib import Path
import ast
import requests
import sys
import runpy

#sys.argv = ['abc.py','full', 'arg2']    #Used only for debugging. Should be commented in module



#For history
year = ''
if year=='': folder = ''
else: folder = f'history/{year}/'

#List of useful links
url1 = "https://fantasy.premierleague.com/api/bootstrap-static/" #Each line is for the player with aggregate Data
url2 = "https://fantasy.premierleague.com/api/entry/698498/history/" #Data for FPL manager history
url3 = "https://fantasy.premierleague.com/api/event/6/live/" #Not used
url4 = "https://fantasy.premierleague.com/api/fixtures" #Fixtures Table
url5 = 'https://fantasy.premierleague.com/api/element-summary/191/' #Data for thr player with 'id' 191

#Downloading the Table Which is Used for Players and Teams Lists only
p1 = long_request(url1)  
data1 = p1.text
d1 = json.loads(data1)
bigTable = pd.DataFrame(d1['elements'])
bigTable['full_name'] = bigTable['first_name'] + ' ' + bigTable['second_name']
bigTable[['ep_next', 'ep_this', 'form', 'points_per_game', 'selected_by_percent',
       'value_form', 'value_season', 'influence', 'creativity', 'threat', 'ict_index']] =\
bigTable[['ep_next', 'ep_this', 'form', 'points_per_game', 'selected_by_percent',
       'value_form', 'value_season', 'influence', 'creativity', 'threat', 'ict_index']].apply(pd.to_numeric)
bigTable = bigTable.applymap(lambda x: np.nan if x=='' else x)


#Creating the Lists of Teams, Players and the Dictionary of Correspondence
teams = dict(zip(pd.DataFrame(d1['teams'])['id'],pd.DataFrame(d1['teams'])['name']))
players = dict(zip(bigTable['id'],bigTable['full_name']))
teamplayers = dict(zip(bigTable['id'],bigTable['team']))
pos_dict = {1: 'G', 2: 'D', 3: 'M', 4: 'F'}
positions = dict(zip(bigTable['id'],[pos_dict[bigTable.at[i,'element_type']] for i in bigTable.index]))
team_number = len(teams)

#Downloading the Fixtures Table
p4 = long_request(url4) 
d4 = json.loads(p4.text)

#In case data are updating or some other bug
try:
    Fixtures = pd.DataFrame(d4)
    Fixtures = Fixtures[Fixtures.columns.sort_values()] #Give strict(alphabetical) order
    do_smth = True
except: 
    print("Data are probably updating or some other bug")
    sys.argv[1] = 'nothing'
    do_smth = False

if do_smth:
    ''' Checking if anything has changed since previous download
    
        If nothing has changed - nothing
        If only players/team data - medium
        If fixtures has changed - full
    '''
    if sys.argv[1] != 'full':
        bigTable_old = pd.read_csv('in/bootstrap.csv')
        Fixtures_old = pd.read_csv('in/Fixtures.csv')
        
        #Copy from file making lists - lists (not str)
        Fixtures_old = Fixtures_old.apply(lambda x: [ast.literal_eval(x[i]) for i in range(len(x))] \
        if x.name=='stats' else x)
        
        Large_Table_old = pd.read_csv('in/LTable_FPL.csv')
        F_diff = differences(Fixtures_old[['event','id']], Fixtures[['event','id']])
        L_diff = differences(bigTable[['team']], bigTable_old[['team']])
        if (len(L_diff) > 0)|(len(F_diff) > 0) > 0:
            sys.argv[1] = 'full'
            if len(F_diff) > 0:
                print(f'Fixtures has changed:')
                display(F_diff)
            if len(L_diff) > 0:
                print(f'Players teams list has changed:')
                display(L_diff)
        else:
            F_diff_full = differences(Fixtures_old, Fixtures)
            L_diff_full = differences(bigTable[['team', 'creativity', 'threat', 'minutes', 'points_per_game']],\
            bigTable_old[['team', 'creativity', 'threat', 'minutes', 'points_per_game']])
            
            if (len(L_diff_full) > 0)|(len(F_diff_full) > 0):
                sys.argv[1] = 'medium'
                if len(F_diff_full) > 0:
                    print('Medium Fixtures data has changed:')
                    display(F_diff_full)
                if len(L_diff_full) > 0:
                    print('Medium Players data has changed:')
                    display(L_diff_full)
    if not (sys.argv[1] in {'medium', 'full'}):
        sys.argv[1] = 'nothing'

        print(f'\tChecking equalities is over.\t It takes {str(time() - start)} sec\n')
        start = time()

print(f'\t\tData Collection Type = {sys.argv[1]}\n')
        
        
        
        
# If sys.argv[1] == 'nothing', nothing should be really done till the end of the file
if sys.argv[1] != 'nothing':
    
    
    #Creating the Main Table for FPL Source
    Table = pd.DataFrame()
    for i in bigTable['id']:
        url = 'https://fantasy.premierleague.com/api/element-summary/'+str(i)+'/'
        p = long_request(url)
        d = json.loads(p.text)
        dd = pd.DataFrame(d['history'])
        Table = Table.append(dd, ignore_index=True)
        
    #Checking that Table is not empty (start of the season)
    if 'threat' in Table.columns:
        Table['name'] = [players[Table.at[i, 'element']] for i in Table.index]
        #Table['threat'] = pd.to_numeric(Table['threat'])
        #Table['creativity'] = pd.to_numeric(Table['creativity'])
        Table[['creativity', 'threat', 'ict_index', 'influence']]  =\
        Table[['creativity', 'threat', 'ict_index', 'influence']].apply(pd.to_numeric)
        #Table['team'] = [teamplayers[Table.at[i,'element']] for i in Table.index]
        Table['team'] = [Fixtures[Fixtures['id']==Table.at[i,'fixture']]['team_h'].values[0] if Table.at[i,'was_home']\
        else Fixtures[Fixtures['id']==Table.at[i,'fixture']]['team_a'].values[0] for i in Table.index]
        Table['position'] = [positions[Table.at[i,'element']] for i in Table.index]
    Table = Table[Table.columns.sort_values()] #Give strict(alphabetical) order
    if round in Table.columns:
        lastGW = Table['round'].max()
    else:
        lastGW = 0

    #Deleteting double gameweeks for players changeg one PL cloub for another during GW ("Walcott case")
    #And matches from the current GW that are not played yet
    indexes_to_drop = []
    #Deleting 'Walcott case'
    for i in Table.index:#[:-1]:
        if i!= len(Table) - 1:
            if (Table['element'][i]==Table['element'][i+1])&(Table['round'][i]==Table['round'][i+1])&\
            (Table['team'][i]!=Table['team'][i+1]):
                if Table['minutes'][i] ==  0:
                    indexes_to_drop.append(i)
                elif Table['minutes'][i+1] ==  0:
                    indexes_to_drop.append(i+1)
                    
    '''
        Large_Table is needed only for inputUndersat. To get FPL names when Understat data is already calculated
        but FPL data is not. So players played are not excluded from the table but have zero data.
    '''                
    Large_Table = Table.drop(indexes_to_drop).reset_index()
    
    
#     if indexes_to_drop != []:
#         '''
#             Large_Table is needed only for inputUndersat. To get FPL names when Understat data is already calculated
#             but FPL data is not. So players played are not excluded from the table but have zero data.
#         '''
#         Large_Table = Table.drop(indexes_to_drop).reset_index()
    
    print(f'\tDownloading FPL tables is over.\t It takes {str(time() - start)} sec\n')
    start = time()
    
    
    
    
    
    # Creating (Little_)Table. Deleting not played
    for i in Table.index:
        if (Fixtures[Fixtures['id'] == Table.at[i,'fixture']]['finished'].values[0] == False)&(not i in indexes_to_drop):
            indexes_to_drop.append(i)
    if indexes_to_drop != []:
        Table = Table.drop(indexes_to_drop).reset_index()        

    #Making Teams Template Table
    Teams = pd.DataFrame()
    Teams['id'] = pd.DataFrame(d1['teams'])['id']
    Teams['Teams'] = pd.DataFrame(d1['teams'])['name']
    Teams['XXX Target number XXX'] = np.zeros(team_number)
    Teams['Matches'] = [len(Fixtures[Fixtures['finished']&((Fixtures['team_a']==i)|(Fixtures['team_h']==i))]) \
                                  for i in range(1,team_number+1)]

    #Making Players Template Table
    Players = pd.DataFrame()
    Players['id'] = bigTable['id']
    Players['Name'] = strip_accents_pdlist(pd.DataFrame(bigTable['full_name']))
    Players['web_name'] = strip_accents_pdlist(pd.DataFrame(bigTable['web_name']))
    Players['Team number'] = [bigTable[bigTable['id'] == i]['team'].sum() for i in Players['id']]
    Players['Team'] = [dict(zip(pd.DataFrame(d1['teams'])['id'],pd.DataFrame(d1['teams'])['name']))\
                       [Players.at[i,'Team number']] for i in Players.index]
    Players['Position'] = bigTable['element_type']


    if  not Table.empty:
        Players['Team games'] = [len(Table[(Table['element']==i) ]) for i in Players['id']]
        Players['Played'] = [len(Table[(Table['element']==i)&(Table['minutes']>0)]) \
                            for i in Players['id']]


    #Calculating Fixtures and Opponents
    Fixtures.to_csv(Path(f'{folder}in/Fixtures.csv'), index=False)
    Table.to_csv(Path(f'{folder}in/Table_FPL.csv'), index=False)
    Teams.to_csv(Path(f'{folder}in/Teams.csv'), index=False)
    Players.to_csv(Path(f'{folder}in/Players.csv'), index=False)
    
    if sys.argv[1] == 'full':
        runpy.run_module('Optional', run_name='smth')

    if sys.argv[1] == 'medium':
        Team_fixtures = pd.read_csv(Path('in/Team_fixtures.csv'))
        Team_played_fixtures = Team_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
        if Fixtures[Fixtures['id']==x]['finished'].iloc[0] else np.nan)
        Team_upcoming_fixtures = Team_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
        if Fixtures[Fixtures['id']==x]['finished'].iloc[0]==False else np.nan)
        
        Team_played_fixtures.to_csv(Path('in/Team_played_fixtures.csv'), index=False)
        Team_upcoming_fixtures.to_csv(Path('in/Team_upcoming_fixtures.csv'), index=False)
        
        
        
        Player_fixtures = pd.read_csv(Path('in/Player_fixtures.csv'))
        Player_played_fixtures = Player_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
        if Fixtures[Fixtures['id']==x]['finished'].iloc[0] else np.nan)
        Player_upcoming_fixtures = Player_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
        if Fixtures[Fixtures['id']==x]['finished'].iloc[0]==False else np.nan)
        
        Player_played_fixtures.to_csv(Path('in/Player_played_fixtures.csv'), index=False)
        Player_upcoming_fixtures.to_csv(Path('in/Player_upcoming_fixtures.csv'), index=False)


    #Writing Tables to csv
    bigTable.to_csv(Path('in/bootstrap.csv'), index=False)
    Large_Table.to_csv(Path('in/LTable_FPL.csv'), index=False)

    
    print(f'\tCreating tables is over.\t It takes {str(time() - start)} sec\n')
    print('inputFPL is over.\t It takes ' + str(time() - start_module) + ' sec\n')

if __name__ == '__main__':
    display('End')