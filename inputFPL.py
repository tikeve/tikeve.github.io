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

#sys.argv = ['abc.py','full', 'arg2']
print(f'System var is - {sys.argv[1]}')
# Reading what type of downloading should be done
if sys.argv[1] == 'full':
    data_collection_type = 'full'
elif sys.argv[1] == 'medium':
    data_collection_type = 'medium'
else: data_collection_type = 'nothing'

#For history
year = ''
if year=='': folder = ''
else: folder = f'history/{year}/'

#List of useful links
url1 = "https://fantasy.premierleague.com/api/bootstrap-static/" #Each line is for the player with aggregate Data
url2 = "https://fantasy.premierleague.com/api/entry/698498/history/" #Data for FPL manager history
url3 = "https://fantasy.premierleague.com/api/event/6/live/" #Not used
url4 = "https://fantasy.premierleague.com/api/fixtures" #Fixtures Table
url5 = 'https://fantasy.premierleague.com/api/element-summary/191/' #Data for fixture 191

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
    data_collection_type = 'nothing'
    do_smth = False

if do_smth:
#     #Calculating Last Gameweek which has already started
#     firstr = len(Fixtures)+1
#     lastr = 0
#     for i in range(len(Fixtures)):
#         if Fixtures.at[i,'finished']==True:
#             firstr = min(firstr, i)
#             lastr = i
#     if firstr < len(Fixtures)+1:
#         lastGW = int(Fixtures.at[lastr,'event'])
#     else: lastGW = 0
#     if lastr == len(Fixtures):
#         lastGW = int(Fixtures.at[lastr,'event'])

    
    
    #Checking if anything has changed since previous download
    '''
        If nothing has changed - nothing
        If only players/team data - medium
        If fixtures has changed - full
    '''
    if data_collection_type == 'nothing':
        bigTable_old = pd.read_csv('in/bootstrap.csv')
        Fixtures_old = pd.read_csv('in/Fixtures.csv')
        
        #Copy from file making lists - lists (not str)
        Fixtures_old = Fixtures_old.apply(lambda x: [ast.literal_eval(x[i]) for i in range(len(x))] \
        if x.name=='stats' else x)
        
        Large_Table_old = pd.read_csv('in/LTable_FPL.csv')
        F_diff = differences(Fixtures_old[['event','id']], Fixtures[['event','id']])
        L_diff = differences(bigTable[['team']], bigTable_old[['team']])
        if (len(L_diff) > 0)|(len(F_diff) > 0) > 0:
            data_collection_type = 'full'
            print(f'\t\tdata_collection_type = {data_collection_type}\n')
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
                data_collection_type = 'medium'
                print(f'\t\tdata_collection_type = {data_collection_type}\n')
                if len(F_diff_full) > 0:
                    print('Medium Fixtures data has changed:')
                    display(F_diff_full)
                if len(L_diff_full) > 0:
                    print('Medium Players data has changed:')
                    display(L_diff_full)

        print(f'\tChecking equalities is over.\t It takes {str(time() - start)} sec\n')
        print(f'\tdata_collection_type = {data_collection_type}\n')
        start = time()

        
        
        
        
        
# If data_collection_type == 'nothing', nothing should be really done till the end of the file
if data_collection_type != 'nothing':
    
    
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
    Table = Table[Table.columns.sort_values()] #Give strict(alphabetical) order
    lastGW = Table['round'].max()

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
    if indexes_to_drop != []:
        '''
            Large_Table is needed only for inputUndersat. To get FPL names when Understat data is already calculated
            but FPL data is not. So players played are not excluded from the table but have zero data.
        '''
        Large_Table = Table.drop(indexes_to_drop).reset_index()
    
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
    #Players['Team games'] = [Teams.at[Players.at[i,'Team number']-1,'Matches'] for i in Players.index]


    if  not Table.empty:
        Players['Team games'] = [len(Table[(Table['element']==i) ]) for i in Players['id']]
        Players['Played'] = [len(Table[(Table['element']==i)&(Table['minutes']>0)]) \
                            for i in Players['id']]


    #print('\t Downloads FPL is over.\t It takes ' + str(time() - start) + ' sec')
    #start = time()

    #Calculating Fixtures and Opponents
    Fixtures.to_csv(Path('in/Fixtures.csv'), index=False)
    Table.to_csv(Path('in/Table_FPL.csv'), index=False)
    Teams.to_csv(Path('in/Teams.csv'), index=False)
    Players.to_csv(Path('in/Players.csv'), index=False)
    if data_collection_type == 'full':
        runpy.run_module('Optional', run_name='smth')
#     '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
#     Team_all = pd.DataFrame()
#     Team_opponent_team = pd.DataFrame()
#     for j in range(int(Fixtures['event'].max()),0,-1): 
#         '''
#             Team_all - contains [number of fixture, home team id, away team id]
#             Team_fixtures - contains [number of fixture]
#             Team_played_fixtures - contains [number of future fixture], future or empty GW deleted
#             Team_upcoming_fixtures - contains [number of played fixture], played or empty GW deleted
#             Team_opponent_team - [opponent team id]
#         '''

#         Team_all['GW'+str(j)] = [Fixtures[((Fixtures['team_a']==i)|(Fixtures['team_h']==i))&(Fixtures['event']==j)]\
#         [['id', 'team_h', 'team_a']].values for i in range(1, team_number+1)]

#         Team_opponent_team['GW'+str(j)] = [[pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][0]\
#         if pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][0] != i+1\
#         else pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][1]\
#         for v in range(len(pd.DataFrame(Team_all.at[i,'GW'+str(j)])))] for i in Team_all.index]   

#     Team_fixtures = Team_all.applymap(lambda x: [x[i][0] for i in range(len(x))])
    
#     #calculating home/away table with 1/0 and NaN
#     Team_home = no_lists(Team_all.applymap(lambda x: list(x))).applymap(lambda x: np.nan if type(x)==float else x[1]-1).\
#     apply(lambda x: x==list(range(len(Team_all)))).applymap(lambda x: 1 if x else 0)\
#     +no_lists(Team_fixtures)-no_lists(Team_fixtures)#to add NaN

#     Team_home = Team_home[Team_home.columns[::-1]]#making right order
    '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    if data_collection_type == 'medium':
        Team_fixtures = pd.read_csv(Path('in/Team_fixtures.csv'))
        Team_played_fixtures = Team_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
        if Fixtures[Fixtures['id']==x]['finished'].iloc[0] else np.nan)
        Team_upcoming_fixtures = Team_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
        if Fixtures[Fixtures['id']==x]['finished'].iloc[0]==False else np.nan)
        
        Team_played_fixtures.to_csv(Path('in/Team_played_fixtures.csv'), index=False)
        Team_upcoming_fixtures.to_csv(Path('in/Team_upcoming_fixtures.csv'), index=False)
        
        
    Team_opponent_team = pd.read_csv(Path('in/Team_opponent_team.csv'))
    Team_home = pd.read_csv(Path('in/Team_home.csv'))
    
    'XXXXXXXXXXXXXXXXXXXXXXXX'
    Team_upcoming_fixtures = pd.read_csv(Path('in/Team_upcoming_fixtures.csv'))
    Team_opponent_team = to_lists(Team_opponent_team[Team_opponent_team.columns[::-1]])
    Team_upcoming_fixtures = to_lists(Team_upcoming_fixtures[Team_upcoming_fixtures.columns[::-1]])
    'XXXXXXXXXXXXXXXXXXXXXXXXX'
    
    
#     Team_played_fixtures = Team_fixtures.applymap(lambda x:[i for i in \
#     Fixtures[(Fixtures['id'].isin(x))&Fixtures['finished']]['id']])

#     Team_upcoming_fixtures = Team_fixtures.applymap(lambda x:[i for i in \
#     Fixtures[(Fixtures['id'].isin(x))&(Fixtures['finished']==False)]['id']])
#     Team_upcoming_fixtures = Team_upcoming_fixtures[Team_upcoming_fixtures.columns[::-1]]


    #del_empty_col(Team_fixtures) #Deleting empty columns e.g. GW30 - GW38 2019-2020
    #del_empty_col(Team_played_fixtures) #Deleting empty columns e.g. GW30 - GW38 2019-2020
    #del_empty_col(Team_upcoming_fixtures) #Deleting empty columns e.g. GW30 - GW38 2019-2020

    '''
        Player_all - contains [number of fixture, opponent team id] for played fixtures
        PLayer_played_fixtures - contains [number of fixture played], future or empty GW deleted
        PLayer_upcoming_fixtures - contains [number of upcoming fixture], played or empty GW deleted
        Team_opponent_team - [opponent team id]
    '''
    Player_all = pd.DataFrame()
    Player_upcoming_fixtures = pd.DataFrame(columns = Team_upcoming_fixtures.columns)
    Player_opponent_team = pd.DataFrame(columns = Team_opponent_team.columns)
    if  not Table.empty:
        for j in range(lastGW,0,-1):

            Player_all['GW'+str(j)] = [Table[(Table['element']==i)&\
            (Table['round']==j)][['fixture', 'opponent_team']].values for i in Players['id']]

    for i in Players.index:
        Player_upcoming_fixtures = Player_upcoming_fixtures.append(Team_upcoming_fixtures.iloc[Players.at[i,'Team number']-1],\
        ignore_index=True)
        Player_opponent_team = Player_opponent_team.append(Team_opponent_team.iloc[Players.at[i,'Team number']-1],\
        ignore_index=True)

    Player_played_fixtures = Player_all.applymap(lambda x: [x[i][0] for i in range(len(x))])
    Pot = Player_all.applymap(lambda x: [x[i][1] for i in range(len(x))]) #Opponents played against (Player Opponent Team)
    for i in Player_opponent_team.index:
        for j in Player_opponent_team.columns:
            if j in Player_all.columns:
                if int(j[2:])!=lastGW:
                    Player_opponent_team.at[i,j] = Pot.at[i,j]

    def Phome(col):
        '''Function for apply to get home/away for players based on home/away of opposed team'''
        return [np.nan if np.isnan(col[i]) else 1 if Team_home.at[int(col[i])-1, col.name]==0 \
        else 0 for i in range(len(col))]
    Player_home = no_lists(Player_opponent_team).apply(Phome, axis=0)

    Player_home = Player_home[Player_home.columns[::-1]] #making right order

    'XXXXXXXXXXXXXXXXXXXXX'
    Team_upcoming_fixtures = no_lists(Team_upcoming_fixtures[Team_upcoming_fixtures.columns[::-1]])
    'XXXXXXXXXXXXX'

    #Writing Tables to csv
    bigTable.to_csv(Path('in/bootstrap.csv'), index=False)
#     Table.to_csv(Path('in/Table_FPL.csv'), index=False)
    Large_Table.to_csv(Path('in/LTable_FPL.csv'), index=False)
#     Fixtures.to_csv(Path('in/Fixtures.csv'), index=False)
#     Teams.to_csv(Path('in/Teams.csv'), index=False)
#     Players.to_csv(Path('in/Players.csv'), index=False)
#    Team_played_fixtures.to_csv(Path('in/Team_played_fixtures.csv'), index=False)
    'XXXXXXXXXXXXXXXXXXXXXXXX'
    Team_upcoming_fixtures.to_csv(Path('in/Team_upcoming_fixtures.csv'), index=False)
    'XXXXXXXXXXXXXXXXXXXXXXXX'
    Team_home.to_csv(Path(f'{folder}in/Team_home.csv'), index=False)
    Player_home.to_csv(Path(f'{folder}in/Player_home.csv'), index=False)

#     Team_fixtures.to_json(Path('in/Team_fixtures.txt'))
#     Team_played_fixtures.to_json(Path('in/Team_played_fixtures.txt'))
#     Team_upcoming_fixtures.to_json(Path('in/Team_upcoming_fixtures.txt'))
#     Team_opponent_team.to_json(Path('in/Team_opponent_team.txt'))
    #Player_fixtures.to_json(Path('in/Player_fixtures.txt'))
    Player_played_fixtures.to_json(Path('in/Player_played_fixtures.txt'))
    Player_upcoming_fixtures.to_json(Path('in/Player_upcoming_fixtures.txt'))
    Player_opponent_team.to_json(Path('in/Player_opponent_team.txt'))

    
    print(f'\tCreating tables is over.\t It takes {str(time() - start)} sec\n')
    print('inputFPL is over.\t It takes ' + str(time() - start_module) + ' sec\n')

if __name__ == '__main__':
    display('End')