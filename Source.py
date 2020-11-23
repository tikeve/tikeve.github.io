from time import time
import constti
from Brr_functions import toint, noZ, no_lists
from bs4 import BeautifulSoup
import requests
import pandas as pd
import json
import numpy as np
from pathlib import Path
import os

class Source:
    def __init__(self, source, ma_num=0, year=''):
        self.source = source
        print(f'Start Creating {self.source} Tables:')
        start_module = time()
        start = time()
        self.ma_num = ma_num # Number of matches for the moving average
        
#1. Reading nessesry data from files.
        
        if year=='': folder = ''
        else: folder = f'history/{year}/'
        #print(f'folder = {folder}')
        try:
            Table = pd.read_csv(Path(f'{folder}in/Table_'+source+'.csv')) # Main data from source containing rows for each player
        except:
            print('!!!!!!!!')
            Table = pd.DataFrame()
            self.Table = Table
            return
        if len(Table)<1:
            Table = pd.DataFrame()
            self.Table = Table
            return
    
        #in a game with columns 'element', 'round', 'fixture', 'threat', 'creativity', 'team', opponent_team'
        Fixtures = pd.read_csv(Path(f'{folder}in/Fixtures.csv')) # Table of rows for each fixture
        Teams = pd.read_csv(Path(f'{folder}in/Teams.csv')) # Table of rows as teams with columns 'id', 'Teams',
        #'TARGET COL', 'Matches'
        Players = pd.read_csv(Path(f'{folder}in/Players.csv')) # Table of rows as players with columns 'id', 'Name',
        #'Team', Team games', 'Played'
        del Players['web_name'] #This column is needed only for inputUnderstat.py

        #Reading Fxtures and Opponents for Teams and Players
        with open(Path(f'{folder}in/Team_opponent_team.txt'), 'r') as file:
            Team_opponent_team = pd.DataFrame(json.loads(file.read()))
            Team_opponent_team.index = pd.to_numeric(Team_opponent_team.index)
            Team_opponent_team = Team_opponent_team.sort_index()
        with open(Path(f'{folder}in/Player_opponent_team.txt'), 'r') as file:
            Player_opponent_team = pd.DataFrame(json.loads(file.read()))
            Player_opponent_team.index = pd.to_numeric(Player_opponent_team.index)
            Player_opponent_team = Player_opponent_team.sort_index()

        if year=='':
            with open(Path(f'{folder}in/Team_played_fixtures.txt'), 'r') as file:
                Team_played_fixtures = pd.DataFrame(json.loads(file.read()))
                Team_played_fixtures.index = pd.to_numeric(Team_played_fixtures.index)
                Team_played_fixtures = Team_played_fixtures.sort_index()
            with open(Path(f'{folder}in/Player_played_fixtures.txt'), 'r') as file:
                Player_played_fixtures = pd.DataFrame(json.loads(file.read()))
                Player_played_fixtures.index = pd.to_numeric(Player_played_fixtures.index)
                Player_played_fixtures = Player_played_fixtures.sort_index()
            with open(Path(f'{folder}in/Team_upcoming_fixtures.txt'), 'r') as file:
                Team_upcoming_fixtures = pd.DataFrame(json.loads(file.read()))
                Team_upcoming_fixtures.index = pd.to_numeric(Team_upcoming_fixtures.index)
                Team_upcoming_fixtures = Team_upcoming_fixtures.sort_index()
            with open(Path(f'{folder}in/Player_upcoming_fixtures.txt'), 'r') as file:
                Player_upcoming_fixtures = pd.DataFrame(json.loads(file.read()))
                Player_upcoming_fixtures.index = pd.to_numeric(Player_upcoming_fixtures.index)
                Player_upcoming_fixtures = Player_upcoming_fixtures.sort_index()
        else:
            with open(Path(f'{folder}in/Team_fixtures.txt'), 'r') as file:
                Team_played_fixtures = pd.DataFrame(json.loads(file.read()))
                Team_played_fixtures.index = pd.to_numeric(Team_played_fixtures.index)
                Team_played_fixtures = Team_played_fixtures.sort_index()
            with open(Path(f'{folder}in/Player_fixtures.txt'), 'r') as file:
                Player_played_fixtures = pd.DataFrame(json.loads(file.read()))
                Player_played_fixtures.index = pd.to_numeric(Player_played_fixtures.index)
                Player_played_fixtures = Player_played_fixtures.sort_index()
            Team_upcoming_fixtures = pd.DataFrame()
            Player_upcoming_fixtures = pd.DataFrame()

        
        
        
#         #in a game with columns 'element', 'round', 'fixture', 'threat', 'creativity', 'team', opponent_team'
#         Fixtures = pd.read_csv(Path('in/Fixtures.csv')) # Table of rows for each fixture
#         Teams = pd.read_csv(Path('in/Teams.csv')) # Table of rows as teams with columns 'id', 'Teams',
#         #'TARGET COL', 'Matches'
#         Players = pd.read_csv(Path('in/Players.csv')) # Table of rows as players with columns 'id', 'Name',
#         #'Team', Team games', 'Played'
#         del Players['web_name'] #This column is needed only for inputUnderstat.py
        
#         #Reading Fxtures and Opponents for Teams and Players
#         with open(Path('in/Team_played_fixtures.txt'), 'r') as file:
#             Team_played_fixtures = pd.DataFrame(json.loads(file.read()))
#             Team_played_fixtures.index = pd.to_numeric(Team_played_fixtures.index)
#             Team_played_fixtures = Team_played_fixtures.sort_index()
#         with open(Path('in/Team_opponent_team.txt'), 'r') as file:
#             Team_opponent_team = pd.DataFrame(json.loads(file.read()))
#             Team_opponent_team.index = pd.to_numeric(Team_opponent_team.index)
#             Team_opponent_team = Team_opponent_team.sort_index()
#         with open(Path('in/Player_played_fixtures.txt'), 'r') as file:
#             Player_played_fixtures = pd.DataFrame(json.loads(file.read()))
#             Player_played_fixtures.index = pd.to_numeric(Player_played_fixtures.index)
#             Player_played_fixtures = Player_played_fixtures.sort_index()
#         with open(Path('in/Player_opponent_team.txt'), 'r') as file:
#             Player_opponent_team = pd.DataFrame(json.loads(file.read()))
#             Player_opponent_team.index = pd.to_numeric(Player_opponent_team.index)
#             Player_opponent_team = Player_opponent_team.sort_index()
            
#         with open(Path('in/Team_upcoming_fixtures.txt'), 'r') as file:
#             Team_upcoming_fixtures = pd.DataFrame(json.loads(file.read()))
#             Team_upcoming_fixtures.index = pd.to_numeric(Team_upcoming_fixtures.index)
#             Team_upcoming_fixtures = Team_upcoming_fixtures.sort_index()
#         with open(Path('in/Player_upcoming_fixtures.txt'), 'r') as file:
#             Player_upcoming_fixtures = pd.DataFrame(json.loads(file.read()))
#             Player_upcoming_fixtures.index = pd.to_numeric(Player_upcoming_fixtures.index)
#             Player_upcoming_fixtures = Player_upcoming_fixtures.sort_index()
        
        print('\t 1. Reading files from ' + source +' is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()

#2. Defenition of useful constants and funtions
        
        #Teams in Premier League
        team_number = len(Teams)

        #Calculating lastGW - last gameweek with at least one game played
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

        print('\t 2. Constants and Functions are over.\t It takes ' + str(time() - start) + ' sec')
        start = time()

# 3. Calculating team tables

        print('\t 3. Calculating team tables')

        #(1). Creating  a table with average threat and GW threats for teams

        Team_xG = Teams.copy()
        Team_xG.columns = ['id', 'Teams', 'xG av', 'Matches']

        for j in range(lastGW,0,-1):
            Team_xG['GW'+str(j)] = [[] for _ in range(team_number)]
            for i in range(team_number):
                for k in range(len(Team_played_fixtures.at[i, 'GW'+str(j)])):
                    Team_xG.at[i,'GW'+str(j)].append(Table[(Table['fixture']==\
                                        Team_played_fixtures.at[i, 'GW'+str(j)][k])&(Table['team']==i+1)]['threat'].sum())


        Team_xG['xG av'] = [Table[Table['team']==i]['threat'].sum() for i in range(1,team_number+1)] \
            /noZ(Teams['Matches'])
        Team_xG.sort_values('xG av', ascending = False, inplace = True)

        print('\t\t 3.1. Team_xG is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(2). Creating  a table with average creativity and GW creativities for teams

        Team_xA = Teams.copy()
        Team_xA.columns = ['id', 'Teams', 'xA av', 'Matches']

        for j in range(lastGW,0,-1):
            Team_xA['GW'+str(j)] = [[] for _ in range(team_number)]
            for i in range(team_number):
                for k in range(len(Team_played_fixtures.at[i, 'GW'+str(j)])):
                    Team_xA.at[i,'GW'+str(j)].append(Table[(Table['fixture']==\
                                    Team_played_fixtures.at[i, 'GW'+str(j)][k])&(Table['team']==i+1)]['creativity'].sum())


        Team_xA['xA av'] = [Table[Table['team']==i]['creativity'].sum() for i in range(1,team_number+1)] \
            /noZ(Teams['Matches'])
        
        print('\t\t 3.2. Team_xA is over.\t It takes ' + str(time() - start)+ ' sec')
        start = time()
        
        #(3). Creating  a table with average threat allowed by teams and GW threat allowed

        Team_Opponent_xG = Teams.copy()
        Team_Opponent_xG.columns = ['id', 'Teams', 'Opponent xG av', 'Matches']

        for j in range(lastGW,0,-1):
            Team_Opponent_xG['GW'+str(j)] = [[] for _ in range(team_number)]
            for i in range(team_number):
                for k in range(len(Team_played_fixtures.at[i, 'GW'+str(j)])):
                    Team_Opponent_xG.at[i,'GW'+str(j)].append(Table[(Table['fixture']== \
                        Team_played_fixtures.at[i, 'GW'+str(j)][k])&(Table['opponent_team']==i+1)]['threat'].sum())


        Team_Opponent_xG['Opponent xG av'] = [Table[Table['opponent_team']==i]['threat'].sum()
                        for i in range(1,team_number+1)]/noZ(Teams['Matches'])

        threatAllowedAv = Team_Opponent_xG['Opponent xG av'].mean()
        
        print('\t\t 3.3. Team_Opponent_xG is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        class_data = [Teams, Players, Team_played_fixtures, Player_played_fixtures, Team_opponent_team,\
        Player_opponent_team, Team_xG, Team_Opponent_xG, threatAllowedAv, lastGW]\

        #(4). Creating  a table with average adjusted threat and GW threats adj for teams

        Team_xG_Ad = adjustment(Team_xG, class_data)       
        print('\t\t 3.4. Team_xG_Ad is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(5). Creating  a table with average adjusted creativity and GW creativities adj for teams
        
        Team_xA_Ad = adjustment(Team_xA, class_data)
        print('\t\t 3.5. Team_xA_Ad is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(6). Creating  a table with average threat allowed adjusted by teams and GW threat allowed adjusted

        Team_Opponent_xG_Ad = adjustment(Team_Opponent_xG, class_data)
        print('\t\t 3.6. Team_Opponent_xG_Ad is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()


        #(7) Total Team Table

        TableTeams = pd.DataFrame()
        TableTeams['id'] = Teams['id']
        TableTeams['Team'] = Teams['Teams']

        TableTeams['xG adjusted'] = Team_xG_Ad['xG av adj']
        TableTeams['xG'] = Team_xG['xG av']
        TableTeams['xA adjusted'] = Team_xA_Ad['xA av adj']
        TableTeams['xA'] = Team_xA['xA av']
        TableTeams['Opponent xG adjusted'] = Team_Opponent_xG_Ad['Opponent xG av adj']
        TableTeams['Opponent xG'] = Team_Opponent_xG['Opponent xG av']
        
        print('\t\t 3.7. TableTeams is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()

# 4. Calculating pLayer tables
        
        print('\t 4. Calculating player tables')

        #(1) Players Threat

        Player_xG = Players.copy()
        #Player_xG['xG per fixture'] = np.zeros(len(Players))
        #Player_xG['xG per game'] = np.zeros(len(Players))
        Player_xG.insert(Player_xG.columns.get_loc('Team games'), 'xG per fixture',\
        [0.0 for i in Player_xG.itertuples()])
        Player_xG.insert(Player_xG.columns.get_loc('Team games'), 'xG per game',\
        [0.0 for i in Player_xG.itertuples()])

        for j in range(lastGW,0,-1):
            Player_xG['GW'+str(j)] = [[] for _ in range(len(Players))]
            for i in range(len(Players)):
                for k in range(len(Player_played_fixtures.at[i, 'GW'+str(j)])):
                    Player_xG.at[i,'GW'+str(j)].append(Table[(Table['fixture']==\
                        Player_played_fixtures.at[i, 'GW'+str(j)][k])&(Table['element']==\
                        Player_xG.at[i,'id'])]['threat'].sum())
                    Player_xG.at[i,'xG per game'] = Player_xG.at[i,'xG per game'] +\
                        Player_xG.at[i,'GW'+str(j)][k]


        Player_xG['xG per fixture'] = Player_xG['xG per game']/noZ(Players['Team games'])
        Player_xG['xG per game'] = Player_xG['xG per game']/noZ(Players['Played'])
        
        print('\t\t 4.1. Player_xG is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(2) Players Creativity

        Player_xA = Players.copy()
        #Player_xA['xA per fixture'] = np.zeros(len(Players))
        #Player_xA['xA per game'] = np.zeros(len(Players))
        Player_xA.insert(Player_xA.columns.get_loc('Team games'), 'xA per fixture',\
        [0.0 for i in Player_xA.itertuples()])
        Player_xA.insert(Player_xA.columns.get_loc('Team games'), 'xA per game',\
        [0.0 for i in Player_xA.itertuples()])

        for j in range(lastGW,0,-1):
            Player_xA['GW'+str(j)] = [[] for _ in range(len(Players))]
            for i in range(len(Players)):
                for k in range(len(Player_played_fixtures.at[i, 'GW'+str(j)])):
                    Player_xA.at[i,'GW'+str(j)].append(Table[(Table['fixture']==\
                                                Player_played_fixtures.at[i, 'GW'+str(j)][k])&\
                                                (Table['element']==Player_xA.at[i,'id'])]['creativity'].sum())
                    Player_xA.at[i,'xA per game'] = Player_xA.at[i,'xA per game'] +\
                        Player_xA.at[i,'GW'+str(j)][k]


        Player_xA['xA per fixture'] = Player_xA['xA per game']/noZ(Players['Team games'])
        Player_xA['xA per game'] = Player_xA['xA per game']/noZ(Players['Played'])
        
        print('\t\t 4.2. Player_xA is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(3) Players Threat Adjusted
        
        Player_xG_Ad = adjustment(Player_xG, class_data)
        print('\t\t 4.3. Player_xG_Ad is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()

        #(4) PLayers Creativity Adjusted
        
        Player_xA_Ad = adjustment(Player_xA, class_data)    
        print('\t\t 4.4. Player_xA_Ad is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
# 4.99 Creating future tables and opponent tables

        Team_oppxG_coeff = no_lists(Team_opponent_team)
        Team_oppdefence_coeff = no_lists(Team_opponent_team)
        Player_oppxG_coeff = no_lists(Player_opponent_team)
        Player_oppdefence_coeff = no_lists(Player_opponent_team)

        Team_oppxG_coeff = Team_oppxG_coeff.applymap(lambda x: Team_xG_Ad.at[int(x)-1,'xG av adj'] \
        if not np.isnan(x) else np.nan)/threatAllowedAv

        Team_oppdefence_coeff = Team_oppdefence_coeff.applymap(lambda x: \
        Team_Opponent_xG_Ad.at[int(x)-1,'Opponent xG av adj'] if not np.isnan(x) else np.nan)/threatAllowedAv

        Player_oppxG_coeff = Player_oppxG_coeff.applymap(lambda x: Team_xG_Ad.at[int(x)-1,'xG av adj'] \
        if not np.isnan(x) else np.nan)/threatAllowedAv

        Player_oppdefence_coeff = Player_oppdefence_coeff.applymap(lambda x: \
        Team_Opponent_xG_Ad.at[int(x)-1,'Opponent xG av adj'] if not np.isnan(x) else np.nan)/threatAllowedAv

        Team_xxG = Team_oppdefence_coeff.mul(Team_xG_Ad['xG av adj'], axis=0)
        Team_xOxG = Team_oppxG_coeff.mul(Team_Opponent_xG_Ad['Opponent xG av adj'], axis=0)
        Player_xxG = Player_oppdefence_coeff.mul(Player_xG_Ad['xG per game adj'], axis=0)
        
        Team_oppxG_coeff.to_json(Path(f'{folder}mid/{self.source}/Team_oppxG_coeff.txt'))
        Team_oppdefence_coeff.to_json(Path(f'{folder}mid/{self.source}/Team_oppdefence_coeff.txt'))
        Player_oppxG_coeff.to_json(Path(f'{folder}mid/{self.source}/Player_oppxG_coeff.txt'))
        Player_oppdefence_coeff.to_json(Path(f'{folder}mid/{self.source}/Player_oppdefence_coeff.txt'))
        Team_xxG.to_json(Path(f'{folder}mid/{self.source}/Team_xxG.txt'))
        Team_xOxG.to_json(Path(f'{folder}mid/{self.source}/Team_xOxG.txt'))
        Player_xxG.to_json(Path(f'{folder}mid/{self.source}/Player_xxG.txt'))

        Display_Team_xxG = Team_xxG.copy()
        Display_Team_xxG['sum'] = Display_Team_xxG.sum(axis=1)
        Display_Team_xxG['Teams'] =  Teams['Teams']
        Display_Team_xxG = Display_Team_xxG[Display_Team_xxG.columns[::-1]].sort_values('sum', ascending=False)
        del Display_Team_xxG['sum']
        for i in range(1,lastGW+1):
            del Display_Team_xxG[f'GW{i}']
        Display_Team_xxG

        Display_Player_xxG = Player_xxG.copy()
        Display_Player_xxG['sum'] = Display_Player_xxG.sum(axis=1)
        Display_Player_xxG['name'] =  Players['Name']
        Display_Player_xxG = Display_Player_xxG[Display_Player_xxG.columns[::-1]].sort_values('sum', ascending=False)
        del Display_Player_xxG['sum']
        for i in range(1,lastGW+1):
            del Display_Player_xxG[f'GW{i}']
        Display_Player_xxG

        Display_Team_xOxG = Team_xOxG.copy()
        Display_Team_xOxG['sum'] = Display_Team_xOxG.sum(axis=1)
        Display_Team_xOxG['Teams'] = Teams['Teams']
        Display_Team_xOxG = Display_Team_xOxG[Display_Team_xOxG.columns[::-1]].sort_values('sum', ascending=True)
        del Display_Team_xOxG['sum']
        for i in range(1,lastGW+1):
            del Display_Team_xOxG[f'GW{i}']
        Display_Team_xOxG
        
        print('\t 4.99. Creating future tables and opponent tables is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
# 5. Writing tables to files (and variables to class.variables)
        
        # Useful variables for debugging
        self.Table = Table
        self.Fixtures = Fixtures
        self.Teams = Teams
        self.Players = Players
        self.lastGW = lastGW
        
        self.threatAllowedAv = threatAllowedAv
        self.Team_played_fixtures = Team_played_fixtures
        self.Team_opponent_team = Team_opponent_team
        self.Player_played_fixtures = Player_played_fixtures
        self.Player_opponent_team = Player_opponent_team
        
        #Save before modifying while writing into files
        self.TxG = Team_xG.copy()
        self.TxA = Team_xA.copy()
        self.TOxG = Team_Opponent_xG.copy()
        self.TxGA = Team_xG_Ad.copy()
        self.TxAA = Team_xA_Ad.copy()
        self.TOxGA = Team_Opponent_xG_Ad.copy()
        self.TaTe = TableTeams.copy() 
        self.PxG = Player_xG.copy()
        self.PxA = Player_xA.copy()
        self.PxGA = Player_xG_Ad.copy()
        self.PxAA = Player_xA_Ad.copy()
        
        self.Team_xG =\
            write_table(Team_xG, 'Team_xG', 'xG av', self.source, self.ma_num, folder)
        self.Team_xA = (write_table
            (Team_xA, 'Team_xA', 'xA av', self.source, self.ma_num, folder))
        self.Team_Opponent_xG =\
            write_table(Team_Opponent_xG, 'Team_Opponent_xG', 'Opponent xG av', self.source, self.ma_num, folder)
        self.Team_xG_Ad =\
            write_table(Team_xG_Ad, 'Team_xG_Ad', 'xG av adj', self.source, self.ma_num, folder)
        self.Team_xA_Ad =\
            write_table(Team_xA_Ad, 'Team_xA_Ad', 'xA av adj', self.source, self.ma_num, folder)
        self.Team_Opponent_xG_Ad =\
            write_table(Team_Opponent_xG_Ad, 'Team_Opponent_xG_Ad', 'Opponent xG av adj', self.source, self.ma_num, folder)
        self.TableTeams =\
            write_table(TableTeams, 'TableTeams', 'xG adjusted', self.source, self.ma_num, folder)
        
        self.Player_xG =\
            write_table(Player_xG, 'Player_xG', 'xG per fixture', self.source, self.ma_num, folder)
        self.Player_xA =\
            write_table(Player_xA, 'Player_xA', 'xA per fixture', self.source, self.ma_num, folder)
        self.Player_xG_Ad =\
            write_table(Player_xG_Ad, 'Player_xG_Ad', 'xG per fixture adj', self.source, self.ma_num, folder)
        self.Player_xA_Ad =\
            write_table(Player_xA_Ad, 'Player_xA_Ad', 'xA per fixture adj', self.source, self.ma_num, folder)
        
        print('\t 5. Writing to files is over.\t It takes ' + str(time() - start) + ' sec')
        print(f'{self.source} is created./t It takes {time() - start_module} sec')
        start = time()

# 6. Tests for each element of the class
    
    # Checks if all names in the source can be found in fantasy.premierleague.com (FPL)
    def test2FPL(self, folder=''):
        FPL = pd.read_csv(f'{folder}in/Table_FPL.csv')
        Table_Source = pd.read_csv(f'{folder}in/Table_'+self.source+'.csv')
        Mistakes = pd.DataFrame()
        No_Names = pd.DataFrame()
        if self.source == 'FPL':
            return Mistakes, No_Names
        for i in Table_Source.index:
            if Table_Source.at[i,'element'] < 1000000:
                FPLmin = FPL[(FPL['element'] == Table_Source.at[i,'element'])&\
                            (FPL['fixture'] == Table_Source.at[i,'fixture'])]['minutes'].sum()
                if abs(Table_Source.at[i,'minutes'] - FPLmin) > 10:
                    Mistakes = Mistakes.append(pd.DataFrame([[Table_Source.at[i,'player'], Table_Source.at[i,'element'],\
                        Table_Source.at[i,'fixture'], Table_Source.at[i,'minutes'], FPLmin]], columns = ['player',\
                        'element', 'fixture', 'minutes', 'FPL_minutes']), ignore_index=True)
            else:
                No_Names = No_Names.append(pd.DataFrame([[Table_Source.at[i,'player'], Table_Source.at[i,'team_name'], Table_Source.at[i,'fixture']]],\
                                                         columns = ['player', 'team_name', 'fixture']), ignore_index=True)
                #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        display(Mistakes)
        display(No_Names)
        return Mistakes, No_Names
        
    def test(self, year=''):
        if year=='': folder = ''
        else: folder = f'history/{year}/'
        if self.Table.empty:
            print(f'Nothing to test. {self.source} table is empty.')
            return
        start = time()
        
        constti.DRDC(self.Team_xG)
        constti.DRDC(self.Team_xA)
        constti.DRDC(self.Team_Opponent_xG)
        constti.DRDC(self.Team_xG_Ad)
        constti.DRDC(self.Team_xA_Ad)
        constti.DRDC(self.Team_Opponent_xG_Ad)
        constti.DRDC(self.TableTeams)
        constti.DRDC(self.Player_xG)
        constti.DRDC(self.Player_xA)
        constti.DRDC(self.Player_xG_Ad)
        constti.DRDC(self.Player_xA_Ad)
        
        self.test2FPL(folder='')
        
        print(f'\t {self.source} Testing is over.\t It takes {time() - start} sec\n\n')

# 7. Useful not class functions

def html_table_name(table_name):
    '''
    Converts the name of the table into the name of the Tale in html file
    '''
    return table_name.replace('Team_', 'Teams Ranking Based on Team ').replace('Player_', 'Players Ranking Based on Player ')\
    .replace('Ad', 'Adjusted').replace('_', ' ').replace('TableTeams', 'Teams Ranking')

# Writing final tables to files and returning table itself and MA variant also
# df - Table to make final table out of it, name - the name of the table, key_col - column to sort,
# source - Understat or FPL, ma_num - number for MA(currently unused just calculated)
def write_table(df, name, key_col, source, ma_num, folder):
    
    #print(f'folder = {folder}')
    df.to_json(Path(f'{folder}mid/{source}/{name}.txt')) #writing table to "mid" folder for advanced calculations
    def table2string(table):
        '''
        Converts table to view suitable for html. All number columns are actually strings with 1 digit after .
        '''
        df = table.copy()
        for j in df.columns:
            if ('GW' in j)|(' av' in j)|('xG' in j)|('xA' in j):
                df[j] = [str(int(df.at[i,j]*10)/10) if not np.isnan(df.at[i,j]) else '' for i in df.index]

        return df
    
    #Removes redundant columns
    del df['id']
    if 'Team number' in df.columns:
        del df['Team number']
    
    # Sort decreasing for attack an increasing for defence
    if 'Opponent' in name:
        df.sort_values(key_col, ascending = True, inplace = True)
    else:
        df.sort_values(key_col, ascending = False, inplace = True)
    
    # Removes lists in celss which are needed for double gameweeks
    df.index = np.arange(1, len(df) + 1)
    df = no_lists(df)
    
    # If moving average number is 0 no calculation needed. Use it to fasten the debug!
    if ma_num == 0:
        df_ma = df
    else:
        df_ma = MA(df, ma_num)
    
    #Writes result to file
    df.to_csv(Path(f'{folder}out/' + source + '/' + name + '.csv'))
    
    
    # Add table created as table of stringe to existing html file replacing table tag with a new one
    #Also adding a new css
    if folder == '':
        dfString = table2string(df)
        
        html_table = dfString.style.apply(color_table, axis=None).render().replace('\n', '')#.format(digit_dict)
        BS_table = BeautifulSoup(html_table, 'html.parser')
        css = str(BS_table('style')[0])
        html_ = str(BS_table('table')[0])

        #Getting right paths for different tables
        if source=='FPL' and name=='TableTeams':
            html_path = Path(f'{folder}index.html')
            css_path = Path(f'{folder}html/FPL/css/TableTeams.css')
        else:
            html_path = Path(f'{folder}html/{source}/{name}.html')
            css_path = Path(f'{folder}html/{source}/css/{name}.css')

        #Making css for coloring butons

        css = ('#' + source + '-button {\n background-color: #82f5cf;\n}\n' +
               '#' + name + ' {\n background-color: lightgrey;\n}\n' + 
               css)

        #Rewriting html and css
        with open(f'{folder}index.html', 'r', encoding="utf-8") as file:
            old_file = file.read()
        tag_to_replace1 = str(BeautifulSoup(old_file, 'html.parser')('table')[0])
        tag_to_replace2 = str(BeautifulSoup(old_file, 'html.parser')('h2')[0])
        new_file = old_file.replace(tag_to_replace1, html_)
        new_file = new_file.replace(tag_to_replace2, f'<h2> {html_table_name(name)} </h2>')

        #Making links right

        if not(source=='FPL' and name=='TableTeams'):
            new_file = new_file.replace('html/FPL/', '')
            new_file = new_file.replace('"out/FPL/TableTeams.csv"', f'"../../out/{source}/{name}.csv"')#Change download button
            new_file = new_file.replace(f'TableTeams', name)
            if name != 'TableTeams':
                new_file = new_file.replace('form action="index.html"', f'form action="../FPL/{name}.html"')
            else:
                new_file = new_file.replace('form action="index.html"', f'form action="../../index.html"')
            new_file = new_file.replace('html/', '../')
            new_file = new_file.replace('"index.html"', '"../../index.html"')

        #Creating .html and special files for the Table
        with open(html_path, 'w', encoding="utf-8") as file:
            file.write(new_file)
        with open(css_path, 'w', encoding="utf-8") as file:
            file.write(css)
    
    return df


#Makes colored tables
def color_table(df):
    def table_type(df):
        if df.columns[2]=='xG': return 'TableTeams'
        elif 'Opponent' in df.columns[1]: return 'Defence'
        elif df.columns[0]=='Teams': return 'Team'
        else: return 'Player'
    def color_Team_xG(val):
        if val=='': color = 'white'
        elif float(val)>200: color = '#09bb9f'
        elif float(val)>140: color = '#82f5cf'
        elif float(val)>120: color = '#c4c4c4'
        else: color = '#15607a'
        return f'Background-color: {color}'
    def color_Player_xG(val):
        if val=='': color = 'white'
        elif float(val)>50: color = '#09bb9f'
        elif float(val)>25: color = '#82f5cf'
        elif float(val)>10: color = '#c4c4c4'
        else: color = '#15607a'
        return f'Background-color: {color}'
    def color_Defence(val):
        if val=='': color = 'white'
        elif float(val)<120: color = '#09bb9f'
        elif float(val)<150: color = '#82f5cf'
        elif float(val)<180: color = '#c4c4c4'
        else: color = '#15607a'
        return f'Background-color: {color}'
    
    #function for style.apply by rows where x is a table type
    def color_table_row(row, x):
        if x=='Team':
            return [color_Team_xG(i) if not(row.name in ['Matches', 'Teams']) else 'Background-color: white' for i in row]
        elif x=='Player':
            return [color_Player_xG(i) if not (row.name  in ['Team games', 'Played', 'Name', 'Team'])\
                    else 'Background-color: white' for i in row]
        elif x=='Defence':
            return [color_Defence(i) if not(row.name in ['Matches', 'Teams']) else 'Background-color: white' for i in row]
        elif x=='TableTeams':
            return [color_Team_xG(i) if row.name in ['xG adjusted', 'xG', 'xA', 'xA adjusted']\
                   else 'Background-color: white' if row.name in ['Team'] else color_Defence(i) for i in row]
        
    return df.apply(color_table_row, x=table_type(df), axis=0)


# Returns the table with d mean average for table Out_T
# If less than d matches played returns zero. If no match played in gameweeek previous gameweek taken.
def MA(Out_T, d):
    
    # Filling the column with averages. Subfunction for MA
    #Adds d - averages for j-th GW_column (not GWj but j-th in order) of table T
    def d_av(T, j, GW_columns, d):
        T.insert(T.columns.get_loc(GW_columns[-1]), f'{GW_columns[j]} {str(d)} - average', [0.0 for i in T.itertuples()])
        for i in T.index:
            u = 0
            k = 0
            while (u < d)&(j-k>=0):
                if not np.isnan(T.at[i, GW_columns[j-k]]):#!!!'':
                    T.at[i, f'{GW_columns[j]} {str(d)} - average'] += T.at[i, GW_columns[j-k]]
                    u+=1
                    k+=1
                else: k+=1
            if u==d:
                T.at[i, f'{GW_columns[j]} {str(d)} - average'] = T.at[i, f'{GW_columns[j]} {str(d)} - average']/d
            else:
                T.loc[i, f'{GW_columns[j]} {str(d)} - average'] = ''
        return None
    
    T = Out_T.copy()
    
    #Creating the list of GW_columns for the table T
    GW_columns = []
    gw_col=0
        
    for col in T.columns:
        if 'GW' in col:
            GW_columns.append(col)
            gw_col+=1
    GW_columns = [GW_columns[i] for i in range(len(GW_columns)-1, -1, -1)] #makes the opposite order of the list
    
    #returning colored MA table if it is TableTeams
    if GW_columns == []: return T
    
    #Add averages for GW_columns using d_av function defined above returning colored MA table if it is not TableTeams
    # Add MA columns only id d not larger than GW played
    if d<=gw_col:
        for j in range(gw_col-1, d-2, -1):
            d_av(T, j, GW_columns, d)

    return T
    
    
######################################For Debugging
# lastGW = 43
# team_number = 20
# Fixtures = Understat.Fixtures
# Table = Understat.Table
# Players = Understat.Players
# Teams = Understat.Teams
# threatAllowedAv = Understat.threatAllowedAv
# Team_Opponent_xG = Understat.TD
# Team_played_fixtures = Understat.Team_played_fixtures
# Team_opponent_team = Understat.Team_opponent_team
# Player_opponent_team = Understat.Player_opponent_team
# Player_played_fixtures = Understat.Player_played_fixtures
# TeamThreat = Understat.TT
# Player_xA = Understat.PC






def adjustment(df, class_data):
    [Teams, Players, Team_played_fixtures, Player_played_fixtures, Team_opponent_team, Player_opponent_team,\
     Team_xG, Team_Opponent_xG, threatAllowedAv, lastGW]\
    = class_data
    if len(df)==len(Teams):
        dfAd = Teams.copy()
        for i in df.columns:
            if ' av' in i:
                key_par = i[:-3]
        av  = 'av'
        dfAd.columns = ['id', 'Teams', f'{key_par} av adj', 'Matches']
        the_fixtures = Team_played_fixtures
        the_opponent_team = Team_opponent_team
    else:
        dfAd = Players.copy()
        for i in df.columns:
            if 'per fixture' in i:
                key_par = i[:-12]
        av = 'per game'
        
        dfAd.insert(dfAd.columns.get_loc('Team games'), f'{key_par} per fixture adj',\
        [0.0 for i in dfAd.itertuples()])
        dfAd.insert(dfAd.columns.get_loc('Team games'), f'{key_par} per game adj',\
        [0.0 for i in dfAd.itertuples()])
        
        
        the_fixtures = Player_played_fixtures
        the_opponent_team = Player_opponent_team
    if 'Opponent' in key_par:#key_par[-7:] == 'allowed':
        Weighting_table = Team_xG
        col = 'xG av'
    else:
        Weighting_table = Team_Opponent_xG
        col = 'Opponent xG av'
        
    for j in range(lastGW,0,-1):
        dfAd[f'GW{j}'] = [[] for _ in range(len(df))]
        for i in range(len(df)):
             for k in range(len(the_fixtures.at[i, 'GW'+str(j)])):
                dfAd.at[i,f'GW{j}'].append(df.at[i,f'GW{j}'][k]\
                *threatAllowedAv/ Weighting_table.at[the_opponent_team.at[i,f'GW{j}'][k]-1, col])

                dfAd.at[i,f'{key_par} {av} adj'] += dfAd.at[i,f'GW{j}'][k]

    if len(df) == len(Teams): dfAd[f'{key_par} av adj'] = dfAd[f'{key_par} av adj']/noZ(dfAd['Matches'])
    else:
        dfAd[f'{key_par} per fixture adj'] = dfAd[f'{key_par} per game adj']/noZ(dfAd['Team games'])
        dfAd[f'{key_par} per game adj'] = dfAd[f'{key_par} per game adj']/noZ(dfAd['Played'])
    
    
    return dfAd

        
if __name__ == '__main__':
    year = ''
    Understat = Source('Understat', ma_num=7, year=year)
    Understat.test(year)
    FPL = Source('FPL', ma_num=7, year=year)
    FPL.test(year)
    #display(MA(Understat.TeamThreatAd, 8))
    pass