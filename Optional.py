''' Rarely needed calculations (Optional)

    Calculate tables if Fixtures changes(sys.argv[1] == 'full')
    
    Sources:    '{folder}in/Fixtures.csv'
                '{folder}in/Teams.csv'
                f'{folder}in/Table_FPL.csv'
                f'{folder}in/Players.csv'
                
    Writes:    'in/Team_fixtures.csv'
                'in/Team_opponent_team.csv'
                'in/Team_played_fixtures.csv'
                'in/Team_upcoming_fixtures.csv'
                'mid/Team_home.csv'
                'mid/Team_scored.csv'
                'mid/Team_conceded.csv'

                'in/Player_fixtures.csv'
                'in/Player_opponent_team.csv'
                'in/Player_played_fixtures.csv'
                'in/Player_upcoming_fixtures.csv'
                'mid/Player_home.csv'
                
                'mid/Team_scores.txt'
                

    
'''



import pandas as pd
import numpy as np
from pathlib import Path
from Brr_functions import no_lists, to_lists, del_empty_col, no_last_GW
import constti

# DATA input
year = ''
if year=='': folder = ''
else: folder = f'history/{year}/'

Fixtures = pd.read_csv(Path(f'{folder}in/Fixtures.csv'))
Teams = pd.read_csv(Path(f'{folder}in/Teams.csv'))
Table = pd.read_csv(Path(f'{folder}in/Table_FPL.csv'))
Players = pd.read_csv(Path(f'{folder}in/Players.csv'))


# Team_all, 
Team_all = pd.DataFrame()
Team_opponent_team = pd.DataFrame()
for j in range(1,1 + int(Fixtures['event'].max())):#,0,-1): 
    '''
        Team_all - contains [number of fixture, home team id, away team id]
        Team_fixtures - contains [number of fixture]
        Team_played_fixtures - contains [number of future fixture], future or empty GW deleted
        Team_upcoming_fixtures - contains [number of played fixture], played or empty GW deleted
        Team_opponent_team - [opponent team id]
    '''

#     Team_all['GW'+str(j)] = [Fixtures[((Fixtures['team_a']==i)|(Fixtures['team_h']==i))&(Fixtures['event']==j)]\
#     [['id', 'team_h', 'team_a']].values for i in range(1, len(Teams)+1)]
    
    a = []
    for i in range(1, len(Teams)+1):
        A = Fixtures[((Fixtures['team_a']==i)|(Fixtures['team_h']==i))&(Fixtures['event']==j)]
        a.append(A[['id', 'team_h', 'team_a']].values)
    Team_all['GW'+str(j)] = a
    
    b = []
    for i in Team_all.index:
        a = []
        for v in range(len(pd.DataFrame(Team_all.at[i,'GW'+str(j)]))):
            if pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][0] != i+1:
                a.append(pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][0])
            else:
                a.append(pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][1])
        b.append(a)
    Team_opponent_team['GW'+str(j)] = b
            
#     Team_opponent_team['GW'+str(j)] = [[pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][0]\
#     if pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][0] != i+1\
#     else pd.DataFrame(Team_all.at[i,'GW'+str(j)]).loc[:,1:2].values[v][1]\
#     for v in range(len(pd.DataFrame(Team_all.at[i,'GW'+str(j)])))] for i in Team_all.index]
    

Team_opponent_team = no_lists(Team_opponent_team)#[Team_opponent_team.columns[::-1]])

Team_fixtures = Team_all.applymap(lambda x: [x[i][0] for i in range(len(x))])
Team_fixtures = no_lists(Team_fixtures)#[Team_fixtures.columns[::-1]])
    
#calculating home/away table with 1/0 and NaN
Team_home = no_lists(Team_all).applymap(lambda x: np.nan if type(x)==float else x[1]-1)
Team_home = Team_home.apply(lambda x: x==list(range(len(Team_all)))).applymap(lambda x: 1 if x else 0)
Team_home += Team_fixtures-Team_fixtures#to add NaN

Team_scores = pd.DataFrame()
for j in range(1, 1 + int(Fixtures['event'].max())):
    a = []
    for i in range(1, len(Teams)+1):
        A = Fixtures[((Fixtures['team_a']==i)|(Fixtures['team_h']==i))&(Fixtures['event']==j)]
        a.append(A[['team_h_score','team_a_score']].values)
    Team_scores['GW'+str(j)] = a
#     Team_scores['GW'+str(j)] = [Fixtures[((Fixtures['team_a']==i)|(Fixtures['team_h']==i))&(Fixtures['event']==j)]\
#     [['team_h_score','team_a_score']].values for i in range(1, len(Teams)+1)]


#TSS = Team_scores
Team_scored_home = no_lists(Team_scores).applymap(lambda x:np.nan if type(x)==float else x[0])*Team_home
Team_scored_away = no_lists(Team_scores).applymap(lambda x:np.nan if type(x)==float else x[1])*(1-Team_home)
Team_scored = Team_scored_home + Team_scored_away

Team_conceded_home = no_lists(Team_scores).applymap(lambda x:np.nan if type(x)==float else x[1])*Team_home
Team_conceded_away = no_lists(Team_scores).applymap(lambda x:np.nan if type(x)==float else x[0])*(1-Team_home)
Team_conceded = Team_conceded_home + Team_conceded_away
    
del_empty_col(Team_fixtures)
del_empty_col(Team_scored)
del_empty_col(Team_conceded)

#Same as in "medium"
Team_played_fixtures = Team_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
if Fixtures[Fixtures['id']==x]['finished'].iloc[0] else np.nan)
Team_upcoming_fixtures = Team_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
if Fixtures[Fixtures['id']==x]['finished'].iloc[0]==False else np.nan)








Player_all = pd.DataFrame()
Player_fixtures = pd.DataFrame(columns = Team_fixtures.columns)
Player_upcoming_fixtures = pd.DataFrame(columns = Team_upcoming_fixtures.columns)
Player_opponent_team = pd.DataFrame(columns = Team_opponent_team.columns)
if  not Table.empty:
    lastGW = Table['round'].max()
    for j in range(lastGW,0,-1):

        Player_all['GW'+str(j)] = [Table[(Table['element']==i)&\
        (Table['round']==j)][['fixture', 'opponent_team', 'minutes']].values for i in Players['id']]
Player_all = no_lists(Player_all[Player_all.columns[::-1]])


x = pd.DataFrame(np.nan, index=Player_all.index, columns=Team_fixtures.columns)
Player_played_fixtures = Player_all.applymap(lambda x: x[0] if type(x) in {list, np.ndarray} else np.nan)
Player_minutes = Player_all.applymap(lambda x: x[2] if type(x) in {list, np.ndarray} else np.nan)
Player_upcoming_fixtures = x.apply(lambda y: Team_upcoming_fixtures.iloc[Players.at[y.name,'Team number']-1], axis=1)
Player_fixtures = x.apply(lambda x: Team_fixtures.iloc[Players.at[x.name,'Team number']-1], axis=1)
Player_fixtures[Player_played_fixtures.columns] = Player_played_fixtures
Player_played_fixtures = Player_fixtures.applymap(lambda x: np.nan if np.isnan(x) else x \
if Fixtures[Fixtures['id']==x]['finished'].iloc[0] else np.nan) #adding empty columns for foture matches
Player_fixtures[f'GW{lastGW}'] = [Player_upcoming_fixtures[f'GW{lastGW}'][i] \
if np.isnan(Player_played_fixtures[f'GW{lastGW}'][i]) else Player_played_fixtures[f'GW{lastGW}'][i] \
for i in range(len(Players))]

Player_opponent_team = x.apply(lambda x: Team_opponent_team.iloc[Players.at[x.name,'Team number']-1], axis=1)
#Opponents played against
Pot = no_last_GW(Player_all.applymap(lambda x: x[1] if type(x) in {list, np.ndarray} else np.nan))
Player_opponent_team[Pot.columns]  = Pot[Pot.columns]
#Player_opponent_teamX = Pot.merge(Player_opponent_teamX)
def Phome(col):
    '''Function for apply to get home/away for players based on home/away of opposed team'''
    return [np.nan if np.isnan(col[i]) else 1 if Team_home.at[int(col[i])-1, col.name]==0 else 0 for i in range(len(col))]
Player_home = Player_opponent_team.apply(Phome, axis=0)




#Writing
Team_fixtures.to_csv(Path('in/Team_fixtures.csv'), index=False)
Team_opponent_team.to_csv(Path('in/Team_opponent_team.csv'), index=False)
Team_played_fixtures.to_csv(Path('in/Team_played_fixtures.csv'), index=False)
Team_upcoming_fixtures.to_csv(Path('in/Team_upcoming_fixtures.csv'), index=False)
Team_home.to_csv(Path('mid/Team_home.csv'), index=False)
Team_scored.to_csv(Path('mid/Team_scored.csv'), index=False)
Team_conceded.to_csv(Path('mid/Team_conceded.csv'), index=False)

Player_fixtures.to_csv(Path('in/Player_fixtures.csv'), index=False)
Player_opponent_team.to_csv(Path('in/Player_opponent_team.csv'), index=False)
Player_played_fixtures.to_csv(Path('in/Player_played_fixtures.csv'), index=False)
Player_upcoming_fixtures.to_csv(Path('in/Player_upcoming_fixtures.csv'), index=False)
Player_minutes.to_csv(Path('mid/Player_minutes.csv'), index=False)
Player_home.to_csv(Path('mid/Player_home.csv'), index=False)


Team_scores.to_json(Path('mid/Team_scores.txt'))

'End'
