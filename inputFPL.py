#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Downloadting Data from fantasy.premierleague.com(FPL)
print('Start inputFPL:')
from time import time
start_module = time()
start = start_module

import constti
import Brr_functions
from bs4 import BeautifulSoup
import pandas as pd
import json
import numpy as np
from pathlib import Path

#List of useful links
url1 = "https://fantasy.premierleague.com/api/bootstrap-static/" #Data for line per PLayers with aggregate Data
url2 = "https://fantasy.premierleague.com/api/entry/698498/history/" #Data for FPL manager history
url3 = "https://fantasy.premierleague.com/api/event/6/live/" #Not used
url4 = "https://fantasy.premierleague.com/api/fixtures" #Fixtures Table
url5 = 'https://fantasy.premierleague.com/api/element-summary/191/' #Data for fixture 191

#Downloading the Table Whih is Used for Players and Teams Lists only
p1 = constti.long_request(url1)  
data1 = p1.text
d1 = json.loads(data1)
bigTable = pd.DataFrame(d1['elements'])
bigTable['full_name'] = bigTable['first_name'] + ' ' + bigTable['second_name']

#Creating the Lists of Teams, Players and the Dictionary of Correspondence
teams = dict(zip(pd.DataFrame(d1['teams'])['id'],pd.DataFrame(d1['teams'])['name']))
players = dict(zip(bigTable['id'],bigTable['full_name']))
teamplayers = dict(zip(bigTable['id'],bigTable['team']))
team_number = len(teams)

#Downloading the Fixtures Table
p4 = constti.long_request(url4) 
d4 = json.loads(p4.text)
Fixtures = pd.DataFrame(d4)

#Calculating Last Gameweek which has already started
firstr = len(Fixtures)+1
lastr = 0
for i in range(len(Fixtures)):
    if Fixtures.at[i,'finished']==True:
        firstr = min(firstr, i)
        lastr = i
if firstr < len(Fixtures)+1:
    lastGW = int(Fixtures.at[lastr,'event'])
else: lastGW = 0
if lastr == len(Fixtures):
    lastGW = int(Fixtures.at[lastr,'event'])



#Creating the Main Table for FPL Source
Table = pd.DataFrame()
for i in bigTable['id']:
    url = 'https://fantasy.premierleague.com/api/element-summary/'+str(i)+'/'
    p = constti.long_request(url)
    d = json.loads(p.text)
    dd = pd.DataFrame(d['history'])
    Table = Table.append(dd, ignore_index=True)
Table['threat'] = pd.to_numeric(Table['threat'])
Table['creativity'] = pd.to_numeric(Table['creativity'])
Table['team'] = [teamplayers[Table.at[i,'element']] for i in Table.index]

#Making Teams Template Table
Teams = pd.DataFrame()
Teams['id'] = pd.DataFrame(d1['teams'])['id']
Teams['Teams'] = pd.DataFrame(d1['teams'])['name']
Teams['XXX Target number XXX'] = np.zeros(team_number)
Teams['Matches'] = [len(Fixtures[Fixtures['finished']&((Fixtures['team_a']==i)|(Fixtures['team_h']==i))])                               for i in range(1,team_number+1)]

#Making Players Template Table
Players = pd.DataFrame()
Players['id'] = bigTable['id']
Players['Name'] = constti.strip_accents_pdlist(pd.DataFrame(bigTable['full_name']))
Players['web_name'] = constti.strip_accents_pdlist(pd.DataFrame(bigTable['web_name']))
Players['Team number'] = [bigTable[bigTable['id'] == i]['team'].sum() for i in Players['id']]
Players['Team'] = [dict(zip(pd.DataFrame(d1['teams'])['id'],pd.DataFrame(d1['teams'])['name']))                   [Players.at[i,'Team number']] for i in Players.index]
Players['Team games'] = [Teams.at[Players.at[i,'Team number']-1,'Matches'] for i in Players.index]
Players['Played'] = [len(Table[(Table['element']==i)&(Table['minutes']>0)])                         for i in Players['id']]


print('\t Downloads FPL is over.\t It takes ' + str(time() - start) + ' sec')
start = time()


#Calculating Fixtures and Opponents
            
Team_all = pd.DataFrame()
Team_fixtures = pd.DataFrame()
Team_opponent_team = pd.DataFrame()
for j in range(lastGW,0,-1): 

    Team_all['GW'+str(j)] = [Fixtures[((Fixtures['team_a']==i)|(Fixtures['team_h']==i))&    (Fixtures['event']==j)][['id', 'team_h', 'team_a']].values for i in range(1, team_number+1)]

    Team_fixtures['GW'+str(j)] = [list(pd.DataFrame(Team_all.at[i,'GW'+str(j)])[0]) for i in Team_all.index]

    Team_opponent_team['GW'+str(j)] = [[pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][0]    if pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][0] != i+1    else pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][1]    for v in range(len(pd.DataFrame(Team_all.at[i,'GW'+str(j)])))] for i in Team_all.index]

Player_all = pd.DataFrame()
Player_fixtures = pd.DataFrame()
Player_opponent_team = pd.DataFrame()
for j in range(lastGW,0,-1):

    Player_all['GW'+str(j)] = [Table[(Table['element']==i)&    (Table['round']==j)][['fixture', 'opponent_team']].values for i in Players['id']]

    Player_fixtures['GW'+str(j)] = [list(pd.DataFrame(Player_all.at[i,'GW'+str(j)])[0]) for i in Player_all.index]

    Player_opponent_team['GW'+str(j)] = [list(pd.DataFrame(Player_all.at[i,'GW'+str(j)])[1])    for i in Player_all.index]

print('\t Fixtures are over.\t It takes ' + str(time() - start) + ' sec')


#Writing Tables to csv
Table.to_csv(Path('in/Table_FPL.csv'), index=False)
Fixtures.to_csv(Path('in/Fixtures.csv'), index=False)
Teams.to_csv(Path('in/Teams.csv'), index=False)
Players.to_csv(Path('in/Players.csv'), index=False)


Team_fixtures.to_json('in/Team_fixtures.txt')
Team_opponent_team.to_json('in/Team_opponent_team.txt')
Player_fixtures.to_json('in/Player_fixtures.txt')
Player_opponent_team.to_json('in/Player_opponent_team.txt')


# Team_fixtures.to_csv(Path('in/Team_fixtures.csv'), index=False)
# Team_opponent_team.to_csv(Path('in/Team_opponent_team.csv'), index=False)
# Player_fixtures.to_csv(Path('in/Player_fixtures.csv'), index=False)
# Player_opponent_team.to_csv(Path('in/Player_opponent_team.csv'), index=False)

print('inputFPL is over.\t It takes ' + str(time() - start_module) + ' sec\n')

if __name__ == '__main__':
    display(Table)


# In[ ]:




