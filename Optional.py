import pandas as pd
import numpy as np
from pathlib import Path
from Brr_functions import no_lists, del_empty_col

# DATA input
year = ''
if year=='': folder = ''
else: folder = f'history/{year}/'

Fixtures = pd.read_csv(Path(f'{folder}in/Fixtures.csv'))
Teams = pd.read_csv(Path(f'{folder}in/Teams.csv'))
Table = pd.read_csv(Path('in/Table_FPL.csv'))



# Team_all, 
Team_all = pd.DataFrame()
Team_opponent_team = pd.DataFrame()
for j in range(int(Fixtures['event'].max()),0,-1): 
    '''
        Team_all - contains [number of fixture, home team id, away team id]
        Team_fixtures - contains [number of fixture]
        Team_played_fixtures - contains [number of future fixture], future or empty GW deleted
        Team_upcoming_fixtures - contains [number of played fixture], played or empty GW deleted
        Team_opponent_team - [opponent team id]
    '''

    Team_all['GW'+str(j)] = [Fixtures[((Fixtures['team_a']==i)|(Fixtures['team_h']==i))&(Fixtures['event']==j)]\
    [['id', 'team_h', 'team_a']].values for i in range(1, len(Teams)+1)]

    Team_opponent_team['GW'+str(j)] = [[pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][0]\
    if pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][0] != i+1\
    else pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][1]\
    for v in range(len(pd.DataFrame(Team_all.at[i,'GW'+str(j)])))] for i in Team_all.index]
    

Team_opponent_team = no_lists(Team_opponent_team[Team_opponent_team.columns[::-1]])

Team_fixtures = Team_all.applymap(lambda x: [x[i][0] for i in range(len(x))])
Team_fixtures = no_lists(Team_fixtures[Team_fixtures.columns[::-1]])
    
#calculating home/away table with 1/0 and NaN
Team_home = no_lists(Team_all.applymap(lambda x: list(x))).applymap(lambda x: np.nan if type(x)==float else x[1]-1).\
apply(lambda x: x==list(range(len(Team_all)))).applymap(lambda x: 1 if x else 0)\
+no_lists(Team_fixtures)-no_lists(Team_fixtures)#to add NaN

Team_home = Team_home[Team_home.columns[::-1]]#making right order



Team_scores = pd.DataFrame()
for j in range(int(Fixtures['event'].max()),0,-1): 
    Team_scores['GW'+str(j)] = [Fixtures[((Fixtures['team_a']==i)|(Fixtures['team_h']==i))&(Fixtures['event']==j)]\
    [['team_h_score','team_a_score']].values for i in range(1, len(Teams)+1)]
Team_scores = Team_scores[Team_scores.columns[::-1]]

#TSS = Team_scores

Team_scored = (no_lists(Team_scores.applymap(lambda x: list(x)))\
.applymap(lambda x:np.nan if type(x)==float else x[0])*Team_home) +\
(no_lists(Team_scores.applymap(lambda x: list(x)))\
.applymap(lambda x:np.nan if type(x)==float else x[1])*(1-Team_home))
    
del_empty_col(Team_fixtures)

no_lists(Team_fixtures)

del_empty_col(Team_scored)


# Team_played_fixtures = Team_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
# if Fixtures[Fixtures['id']==x]['finished'].iloc[0] else np.nan)
# Team_played_fixtures
# Team_upcoming_fixtures = Team_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
# if Fixtures[Fixtures['id']==x]['finished'].iloc[0]==False else np.nan)



#Writing
Team_fixtures.to_csv(Path('in/Team_fixtures.csv'), index=False)
Team_opponent_team.to_csv(Path('in/Team_opponent_team.csv'), index=False)
# Team_played_fixtures.to_csv(Path('in/Team_played_fixtures.csv'), index=False)
# Team_upcoming_fixtures.to_csv(Path('in/Team_upcoming_fixtures.csv'), index=False)
Team_home.to_csv(Path('mid/Team_home.csv'), index=False)
Team_scored.to_csv(Path('mid/Team_scored.csv'), index=False)


Team_scores.to_json(Path('mid/Team_scores.txt'))

'End'
# Team_played_fixtures = Team_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
# if Fixtures[Fixtures['id']==x]['finished'].iloc[0] else np.nan)
# Team_played_fixtures
# Team_upcoming_fixtures = Team_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
# if Fixtures[Fixtures['id']==x]['finished'].iloc[0]==False else np.nan)

# Team_played_fixtures