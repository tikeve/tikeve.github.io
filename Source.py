''' Fantasy Tables from Different Sources

    Creates class Source which makes tables and htmls from 2 sources: FPL and Understat. Depending on source
    getting data from inputFPL.py or inputUnderstat.py
    
    Sources:    f'{folder}in/Table_'+source+'.csv'
                f'{folder}in/Fixtures.csv'
                f'{folder}in/Teams.csv'
                f'{folder}in/Players.csv'
                f'{folder}in/Team_played_fixtures.csv'
                f'{folder}in/Team_upcoming_fixtures.csv'
                f'{folder}in/Team_opponent_team.csv'
                'in/Player_opponent_team.csv'
                'in/Player_played_fixtures.csv'
                'in/Player_upcoming_fixtures.csv'
                f'{folder}Team_fixtures.txt'
                f'{folder}Player_fixtures.txt'
                
                'index.html'
                f'html/{source}/{name}.html'
                f'html/{source}/css/{name}.css'
                name in {Team_xG, Team_xA, Team_Opponent_xG, Team_xG_Ad, Team_xA_Ad, Team_Opponent_xG_Ad,
                Player_xG, PLayer_xA, Player_xG_Ad, Player_xA_Ad}
    
    Write:      f'{folder}out/{source}/{name}.csv'

                f'{folder}mid/{self.source}/Team_xxG.csv'
                f'{folder}mid/{self.source}/Team_xOxG.csv'
                f'{folder}mid/{self.source}/Player_xxG.csv'
                
                'mid/{source}/TxG.csv'
                'mid/{source}/TxA.csv'
                'mid/{source}/TOxG.csv'
                'mid/{source}/TxGA.csv'
                'mid/{source}/TxAA.csv'
                'mid/{source}/TOxGA.csv'
                'mid/{source}/PxG.csv'
                'mid/{source}/PxA.csv'
                'mid/{source}/PxGA.csv'
                'mid/{source}/PxAA.csv'
                f'mid/{source}/Team_Attack_weight.csv'
                f'mid/{source}/Team_Defence_weight.csv'
                f'mid/{source}/Player_Attack_weight.csv'
                f'mid/{source}/Player_Defence_weight.csv'
                
                'index.html'
                f'html/{source}/{name}.html'
                f'html/{source}/css/{name}.css'

'''




from time import time
import constti
from Brr_functions import toint, noZ, no_lists, to_lists, del_empty_col, is_finished, get_gw_num
from bs4 import BeautifulSoup
import requests
import pandas as pd
import json
import numpy as np
from pathlib import Path
import os

class Source:
    def __init__(self, source, ma_num=0, year=''):
        '''
            Initialization function
            source = FPL or Understat
            ma_num = numbers in Moving Average. If 0 then no MA and calculations are faster
            year = '' then current, otherwize year of history "2019-2020" for example
        '''
        self.source = source
        print(f'Start Creating {self.source} Tables:')
        start_module = time()
        start = time()
        self.ma_num = ma_num # Number of matches for the moving average
        
#1. Reading nessesry data from files.
        
        #finding folder with data for year entered
        if year=='': folder = ''
        else: folder = f'history/{year}/'
        
        #Catching empty data at the start of the season. Avoiding mistake with reading empty file
#         try:
        Table = pd.read_csv(Path(f'{folder}in/Table_'+source+'.csv')) # Main data. Row for player's played fixture
#         except:
#             print('!!!!!!!!')
#             Table = pd.DataFrame()
#             self.Table = Table
#             return
#         if len(Table)<1:
#             Table = pd.DataFrame()
#             self.Table = Table
#             return
    
        #in a game with columns 'element', 'round', 'fixture', 'threat', 'creativity', 'team', opponent_team'
        Fixtures = pd.read_csv(Path(f'{folder}in/Fixtures.csv')) # Table of rows for each fixture
        Teams = pd.read_csv(Path(f'{folder}in/Teams.csv')) # Table of rows as teams with columns 'id', 'Teams',
        #'TARGET COL', 'Matches'
        Players = pd.read_csv(Path(f'{folder}in/Players.csv')) # Table of rows as players with columns 'id', 'Name',
        #'Team', Team games', 'Played'
        del Players['web_name'] #This column is needed only for inputUnderstat.py

        #Reading Fxtures and Opponents for Teams and Players
        
        Team_played_fixtures = pd.read_csv(Path(f'{folder}in/Team_played_fixtures.csv'))       
        Team_upcoming_fixtures = pd.read_csv(Path(f'{folder}in/Team_upcoming_fixtures.csv'))
        Team_opponent_team = pd.read_csv(Path(f'{folder}in/Team_opponent_team.csv'))
        Player_opponent_team = pd.read_csv(Path(f'{folder}in/Player_opponent_team.csv'))
        #To make code shorter
        Tot = Team_opponent_team
        Pot = Player_opponent_team
        
        #file names differ for the present and history
        if year=='':

            Player_played_fixtures = pd.read_csv(Path(f'{folder}in/Player_played_fixtures.csv'))
            Player_upcoming_fixtures = pd.read_csv(Path(f'{folder}in/Player_upcoming_fixtures.csv'))
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
        
        print('\t 1. Reading files from ' + source +' is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()

#2. Defenition of useful constants and funtions
        
        #Teams in Premier League
        team_number = len(Teams)

        #Calculating lastGW - last gameweek with at least one game played
        lastGW  = Table['round'].max()

        print('\t 2. Constants and Functions are over.\t It takes ' + str(time() - start) + ' sec')
        start = time()

# 3. Calculating team tables

        print('\t 3. Calculating team tables')
    
        def xAxG(col, id, entity, field):
            '''
                Function for apply to *_played_fixtures to make tables
                entity: team, opponent or player
                field: name of the field in Table
            '''
            new_col = []
            if entity == 'player':
                entity = 'element'
            elif entity == 'opponent':
                entity = 'opponent_team'
                
            for i in range(len(col)):
                if np.isnan(col[i]):
                    new_col.append(np.nan)
                else:
                    x = Table[(Table['fixture']==col[i])&(Table[entity]==id[i])][field]
                    
                    if len(x) == 0:
                        new_col.append(0)
                    else:
                        new_col.append(x.sum())
            return new_col

        #(1). Creating  a table with average threat and GW threats for teams
        
        TxG = Team_played_fixtures.apply(xAxG, args=(Teams['id'],'team','threat'))
        
        print('\t\t 3.1. Team_xG is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        
        #(2). Creating  a table with average creativity and GW creativities for teams
        
        TxA = Team_played_fixtures.apply(xAxG, args=(Teams['id'],'team','creativity'))
        
        print('\t\t 3.2. Team_xA is over.\t It takes ' + str(time() - start)+ ' sec')
        start = time()
        
        
        #(3). Creating  a table with average threat allowed by teams and GW threat allowed
        
        TOxG = Team_played_fixtures.apply(xAxG, args=(Teams['id'],'opponent','threat'))
        
        print('\t\t 3.3. Team_Opponent_xG is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        
        #(3.99). Creating coeffitients for team tables adjustment
        
        threatAllowedAv = TOxG.mean(axis=1).mean() #Team_Opponent_xG['Opponent xG av'].mean()
        TOxG_av = TOxG.mean(axis=1)
        TxG_av = TxG.mean(axis=1)
        #Team Attack weight unadjusted
        TAwu = Tot.applymap(lambda x: np.nan if np.isnan(x) else TOxG_av[int(x)-1])/threatAllowedAv
        #Team Defence weight unadjusted
        TDwu = Tot.applymap(lambda x: np.nan if np.isnan(x) else TxG_av[int(x)-1])/threatAllowedAv
        #Player Attack weight unadjusted
        PAwu = 'Define to be defined for "class_args"'
        #Player Defence weight unadjusted
        PDwu = 'Define to be defined for "class_args"'
        
        class_args = [Teams, Players, TAwu, TDwu, PAwu]
        class_args += [self.source, self.ma_num, folder]
        
        print('\t\t 3.3.99 Creating coeffitients is over.\t It takes ' + str(time() - start)+ ' sec')
        start = time()

        
        #(4). Creating  a table with average adjusted threat and GW threats adj for teams

        TxGA, Team_xG_Ad = adjustment('Team_xG', TxG, class_args)

        print('\t\t 3.4. Team_xG_Ad is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(5). Creating  a table with average adjusted creativity and GW creativities adj for teams
        
        TxAA, Team_xA_Ad = adjustment('Team_xA', TxA, class_args)

        print('\t\t 3.5. Team_xA_Ad is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(6). Creating  a table with average threat allowed adjusted by teams and GW threat allowed adjusted

        TOxGA, Team_Opponent_xG_Ad = adjustment('Team_Opponent_xG', TOxG, class_args)

        print('\t\t 3.6. Team_Opponent_xG_Ad is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()


        #(7) Total Team Table

        TableTeams = pd.DataFrame()
        TableTeams['id'] = Teams['id']
        TableTeams['Team'] = Teams['Teams']
        
        TOxGA_av = TOxGA.mean(axis=1)
        TxGA_av = TxGA.mean(axis=1)
        
        TableTeams['xG adjusted'] = TxGA_av#Team_xG_Ad['xG av adj']
        TableTeams['xG'] = TxG_av#TxG.mean(axis=1)#Team_xG['xG av']
        TableTeams['xA adjusted'] = TxAA.mean(axis=1)#Team_xA_Ad['xA av adj']
        TableTeams['xA'] = TxA.mean(axis=1)#Team_xA['xA av']
        TableTeams['Opponent xG adjusted'] = TOxGA_av#Team_Opponent_xG_Ad['Opponent xG av adj']
        TableTeams['Opponent xG'] = TOxG_av#Team_Opponent_xG['Opponent xG av']
        
        print('\t\t 3.7. TableTeams is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()

        
# 4. Calculating pLayer tables
        
        print('\t 4. Calculating player tables')

        #(1) Players Threat
        
        PxG = Player_played_fixtures.apply(xAxG, args=(Players['id'],'player','threat'))
        
        print('\t\t 4.1. Player_xG is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        
        #(2) Players Creativity
        
        PxA = Player_played_fixtures.apply(xAxG, args=(Players['id'],'player','creativity'))
        
        print('\t\t 4.2. Player_xA is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        
        #(2.99) Creating coeffitients for player tables adjustment

        PAwu=Player_opponent_team.applymap(lambda x: np.nan if np.isnan(x) else TOxG_av[int(x)-1])/threatAllowedAv
        PDwu = Player_opponent_team.applymap(lambda x:np.nan if np.isnan(x) else TxG_av[int(x)-1])/threatAllowedAv

        class_args = [Teams, Players, TAwu, TDwu, PAwu]
        class_args += [self.source, self.ma_num, folder]
        
        print('\t\t 4.2.99 Creating coeffitients is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        
        #(3) Players Threat Adjusted
        
        PxGA, Player_xG_Ad = adjustment('Player_xG', PxG, class_args)

        print('\t\t 4.3. Player_xG_Ad is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()

        
        #(4) PLayers Creativity Adjusted
        
        PxAA, Player_xA_Ad = adjustment('Player_xA', PxA, class_args)

        print('\t\t 4.4. Player_xA_Ad is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        
# 5. Creating future tables and opponent tables

        #(1) Calculating futures
        print('\t 5. Creating future tables and opponent tables.')
        
        #Columns with np.nan automatically become float that's why int() is needed
        Team_Defence_weight = Tot.applymap(lambda x: TxGA_av[int(x)-1] if not np.isnan(x) else np.nan)/threatAllowedAv
        Team_Attack_weight = Tot.applymap(lambda x: TOxGA_av[int(x) - 1] if not np.isnan(x) else np.nan)/threatAllowedAv
        Player_Defence_weight = Pot.applymap(lambda x: TxGA_av[int(x)-1] if not np.isnan(x) else np.nan)/threatAllowedAv
        Player_Attack_weight = Pot.applymap(lambda x: TOxGA_av[int(x) - 1] if not np.isnan(x) else np.nan)/threatAllowedAv

        TxxG = Team_Attack_weight.mul(Team_xG_Ad['xG av adj'], axis=0)
        TxxG = Team_upcoming_fixtures.applymap(lambda x: np.nan if np.isnan(x) else 1)*TxxG
        TxxA = Team_Attack_weight.mul(Team_xA_Ad['xA av adj'], axis=0)
        TxxA = Team_upcoming_fixtures.applymap(lambda x: np.nan if np.isnan(x) else 1)*TxxA
        TxOxG = Team_Defence_weight.mul(Team_Opponent_xG_Ad['Opponent xG av adj'], axis=0)
        TxOxG =  Team_upcoming_fixtures.applymap(lambda x: np.nan if np.isnan(x) else 1)*TxOxG
        PxxG = Player_Attack_weight.mul(Player_xG_Ad['xG per game adj'], axis=0)
        PxxG =  Player_upcoming_fixtures.applymap(lambda x: np.nan if np.isnan(x) else 1)*PxxG
        PxxA = Player_Attack_weight.mul(Player_xA_Ad['xA per game adj'], axis=0)
        PxxA =  Player_upcoming_fixtures.applymap(lambda x: np.nan if np.isnan(x) else 1)*PxxA
        PxOxG = Players.apply(lambda x: TxOxG.iloc[x['Team number'] - 1], axis = 1)
        
        class_args = [Teams, Players, TAwu, TDwu, PAwu]
        class_args += [self.source, self.ma_num, folder]
        
        #Writing pure numbers(lists of numbers) tables to mid folder for stats calculations
#         Team_oppxG_coeff.to_json(Path(f'{folder}mid/{self.source}/Team_oppxG_coeff.txt'))
#         Team_oppdefence_coeff.to_json(Path(f'{folder}mid/{self.source}/Team_oppdefence_coeff.txt'))
#         Player_oppxG_coeff.to_json(Path(f'{folder}mid/{self.source}/Player_oppxG_coeff.txt'))
#         Player_oppdefence_coeff.to_json(Path(f'{folder}mid/{self.source}/Player_oppdefence_coeff.txt'))
#         Team_xxG.to_json(Path(f'{folder}mid/{self.source}/Team_xxG.txt'))
#         Team_xOxG.to_json(Path(f'{folder}mid/{self.source}/Team_xOxG.txt'))
#         Player_xxG.to_json(Path(f'{folder}mid/{self.source}/Player_xxG.txt'))

        
        print('\t\t 5.1. Calculating futures.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(2) Visualizing futures
        
        #Adding some words for tables to be readable (lists still)
        Team_xxG = del_empty_col(TxxG.copy())
        Team_xxG['sum'] = Team_xxG.sum(axis=1)
        Team_xxG.insert(0, 'Teams', Teams['Teams'])
        Team_xxG = Team_xxG.sort_values('sum', ascending=False)
        del Team_xxG['sum']
#         for i in range(1,lastGW+1):
#             del Team_xxG[f'GW{i}']
        #Team_xxG

        Player_xxG = del_empty_col(PxxG.copy())
        Player_xxG['sum'] = Player_xxG.sum(axis=1)
        Player_xxG.insert(0, 'Name', Players['Name'])
#         Player_xxG['name'] =  Players['Name']
        Player_xxG = Player_xxG.sort_values('sum', ascending=False)
        del Player_xxG['sum']
#         for i in range(1,lastGW+1):
#             del Player_xxG[f'GW{i}']
#         Player_xxG

        Team_xOxG = del_empty_col(TxOxG.copy())
        Team_xOxG['sum'] = Team_xOxG.sum(axis=1)
        Team_xOxG.insert(0, 'Teams', Teams['Teams'])
#         Team_xOxG['Teams'] = Teams['Teams']
        Team_xOxG = Team_xOxG.sort_values('sum', ascending=True)
        del Team_xOxG['sum']
#         for i in range(1,lastGW+1):
#             del Team_xOxG[f'GW{i}']
#         Team_xOxG
        
        print('\t\t 5.2. Visualizing futures.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
# 6. Writing tables to files (and variables to class.variables)
        
        # Useful variables for debugging
        self.Table = Table
        self.Fixtures = Fixtures
        self.Teams = Teams
        self.Players = Players
        self.lastGW = lastGW
        
        self.class_args = class_args
        self.threatAllowedAv = threatAllowedAv
        self.Team_played_fixtures = Team_played_fixtures
        self.Team_upcoming_fixtures = Team_upcoming_fixtures
        self.Team_opponent_team = Team_opponent_team
        self.Player_played_fixtures = Player_played_fixtures
        self.Player_upcoming_fixtures = Player_upcoming_fixtures
        self.Player_opponent_team = Player_opponent_team
        
        #Save before modifying while writing into files
#         self.Team_xG = Team_xG.copy()
#         self.Team_xA = Team_xA.copy()
#         self.Team_Opponent_xG = Team_Opponent_xG.copy()
#         self.Team_xG_Ad = Team_xG_Ad.copy()
#         self.Team_xA_Ad = Team_xA_Ad.copy()
#         self.Team_Opponent_xG_Ad = Team_Opponent_xG_Ad.copy()
#         self.TableTeams = TableTeams.copy() 
#         self.Player_xG = Player_xG.copy()
#         self.Player_xA = Player_xA.copy()
#         self.Player_xG_Ad = Player_xG_Ad.copy()
#         self.Player_xA_Ad = Player_xA_Ad.copy()
        
        self.TxG = TxG.copy()
        self.TxA = TxA.copy()
        self.TOxG = TOxG.copy()
        self.TxGA = TxGA.copy()
        self.TxAA = TxAA.copy()
        self.TOxGA = TOxGA.copy()
        self.PxG = PxG.copy()
        self.PxA = PxA.copy()
        self.PxGA = PxGA.copy()
        self.PxAA = PxAA.copy()
        
        self.TxxG = TxxG
        self.TxxA = TxxA
        self.TxOxG = TxOxG
        self.PxxG = PxxG
        self.PxxA = PxxA
#         self.Team_xxG = Team_xxG
#         self.Team_xOxG = Team_xOxG
#         self.Player_xxG = Player_xxG
        
        TxG.to_csv(Path(f'{folder}mid/{source}/TxG.csv'), index=False)
        TxA.to_csv(Path(f'{folder}mid/{source}/TxA.csv'), index=False)
        TOxG.to_csv(Path(f'{folder}mid/{source}/TOxG.csv'), index=False)
        TxGA.to_csv(Path(f'{folder}mid/{source}/TxGA.csv'), index=False)
        TxAA.to_csv(Path(f'{folder}mid/{source}/TxAA.csv'), index=False)
        TOxGA.to_csv(Path(f'{folder}mid/{source}/TOxGA.csv'), index=False)
        PxG.to_csv(Path(f'{folder}mid/{source}/PxG.csv'), index=False)
        PxA.to_csv(Path(f'{folder}mid/{source}/PxA.csv'), index=False)
        PxGA.to_csv(Path(f'{folder}mid/{source}/PxGA.csv'), index=False)
        PxAA.to_csv(Path(f'{folder}mid/{source}/PxAA.csv'), index=False)
        
        TxxG.to_csv(Path(f'{folder}mid/{source}/TxxG.csv'), index=False)
        TxxA.to_csv(Path(f'{folder}mid/{source}/TxxA.csv'), index=False)
        TxOxG.to_csv(Path(f'{folder}mid/{source}/TxOxG.csv'), index=False)
        PxxG.to_csv(Path(f'{folder}mid/{source}/PxxG.csv'), index=False)
        PxxA.to_csv(Path(f'{folder}mid/{source}/PxxA.csv'), index=False)
        PxOxG.to_csv(Path(f'{folder}mid/{source}/PxOxG.csv'), index=False)
        
        Team_Attack_weight.to_csv(Path(f'{folder}mid/{source}/Team_Attack_weight.csv'), index=False)
        Team_Defence_weight.to_csv(Path(f'{folder}mid/{source}/Team_Defence_weight.csv'), index=False)
        Player_Attack_weight.to_csv(Path(f'{folder}mid/{source}/Player_Attack_weight.csv'), index=False)
        Player_Defence_weight.to_csv(Path(f'{folder}mid/{source}/Player_Defence_weight.csv'), index=False)
        
#         Team_xxG.to_csv(Path(f'{folder}mid/{self.source}/Team_xxG.csv'), index=False)
#         Team_xOxG.to_csv(Path(f'{folder}mid/{self.source}/Team_xOxG.csv'), index=False)
#         Player_xxG.to_csv(Path(f'{folder}mid/{self.source}/Player_xxG.csv'), index=False)
        

        self.Team_xG = write_table(TxG, 'Team_xG', 'xG av', TxxG, 100, 1, class_args)
        self.Team_xA = write_table(TxA, 'Team_xA', 'xA av', TxxA, 100, 1, class_args)
        self.Team_Opponent_xG = write_table(TOxG, 'Team_Opponent_xG', 'Opponent xG av', TxOxG, 100, 1, class_args)
        self.Team_xG_Ad = write_table(TxGA, 'Team_xG_Ad', 'xG av adj', TxxG, 100, 1, class_args)
        self.Team_xA_Ad = write_table(TxAA, 'Team_xA_Ad', 'xA av adj', TxxA, 100, 1, class_args)
        self.Team_Opponent_xG_Ad = write_table(TOxGA, 'Team_Opponent_xG_Ad','Opponent xG av adj',TxOxG,100,1, class_args)
        self.TableTeams = write_table(TableTeams, 'TableTeams', 'xG adjusted', 'NOTHING', 100, 1, class_args)
        
        self.Player_xG = write_table(PxG, 'Player_xG', 'xG per fixture', PxxG, 100, 1, class_args)
        self.Player_xA = write_table(PxA, 'Player_xA', 'xA per fixture', PxxA, 100, 1, class_args)
        self.Player_xG_Ad = write_table(PxGA, 'Player_xG_Ad', 'xG per fixture adj', PxxG, 100, 1, class_args)
        self.Player_xA_Ad = write_table(PxAA, 'Player_xA_Ad', 'xA per fixture adj', PxxA, 100, 1, class_args)
        
        print('\t 6. Writing to files is over.\t It takes ' + str(time() - start) + ' sec')
        print(f'{self.source} is created./t It takes {time() - start_module} sec')
        start = time()

# 7. Tests for each element of the class
    
    def test2FPL(self, folder=''):
        '''
            Checks if all names in the source can be found in fantasy.premierleague.com (FPL)
            Subfunction of test()
        '''
        FPL = pd.read_csv(f'{folder}in/LTable_FPL.csv')
        Table_Source = pd.read_csv(f'{folder}in/Table_'+self.source+'.csv')
        Mistakes = pd.DataFrame()
        No_Names = pd.DataFrame()
        if self.source == 'FPL':
            return Mistakes, No_Names
        
        col_m = ['player', 'element', 'fixture', 'minutes', 'FPL_minutes']
        col_n = ['player', 'team_name', 'fixture']
        for i in Table_Source.index:
            if Table_Source.at[i,'element'] < 1000000:
                
                C = (FPL['element'] == Table_Source.at[i,'element'])&(FPL['fixture'] == Table_Source.at[i,'fixture'])
                FPLmin = FPL[C]['minutes'].sum()
                
                if abs(Table_Source.at[i,'minutes'] - FPLmin) > 10:
                    a = [Table_Source.at[i,'player'], Table_Source.at[i,'element']]
                    a += [Table_Source.at[i,'fixture'], Table_Source.at[i,'minutes'], FPLmin]
                    Mistakes = Mistakes.append(pd.DataFrame( [a], columns = col_m), ignore_index=True)
            else:
                T = [[Table_Source.at[i,'player'], Table_Source.at[i,'team_name'], Table_Source.at[i,'fixture']]]
                No_Names = No_Names.append(pd.DataFrame( T, columns = col_n), ignore_index=True)
                
        display(Mistakes)
        display(No_Names)
        return Mistakes, No_Names
        
    def test(self, year=''):
        '''
            Tests that all tables are correct (no duplicated columns or rows)
            Uses test2FPL() to compare names
        '''
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
    x = table_name.replace('Team_', 'Teams Ranking Based on Team ')
    x = x.replace('Player_', 'Players Ranking Based on Player ')
    x = x.replace('Ad', 'Adjusted').replace('_', ' ').replace('TableTeams', 'Teams Ranking')
    return x

def write_table(df, name, key_col, xdf, df_n, xdf_n, class_args):
    '''
        Writing final tables to files and returning table itself and MA variant also
        df - Table to make final table out of it, name - the name of the table, key_col - column to sort,
        xdf - Table of predictions
        df_n, xdf_n - number of GWs of the table and expectes table to display
        source - Understat or FPL, ma_num - number for MA(currently unused just calculated)
    '''
    def GW_forecast (df, xdf, df_n, xdf_n):
        '''
            df - Table
            xdf - Table of predictions
            df_n, xdf_n - number of GWs of the table and expectes table to display
        '''
        lastGW = get_gw_num(del_empty_col(df.copy()).columns[-1])
        Table = pd.DataFrame()
        for i in df.columns:
            Table[i] = [df.at[j,i] if np.isnan(xdf.at[j,i]) else xdf.at[j,i] for j in df.index]

#         column_numbers = np.asarray(list(map(get_gw_num, Table.columns)))
#         if max(column_numbers)>lastGW:
#             n = lastGW + 1
#         else:
#             n = lastGW
        for i in Table.columns:
            if (get_gw_num(i) > lastGW + xdf_n)|(get_gw_num(i) <= lastGW - df_n):
                del Table[i]
        return Table 
    def table2string(table):
        '''
        Converts table to view suitable for html. All number columns are actually strings with 1 digit after .
        '''
        df = table.copy()
        for j in df.columns:
            if ('GW' in j)|(' av' in j)|('xG' in j)|('xA' in j):
                df[j] = [str(int(df.at[i,j]*10)/10) if not np.isnan(df.at[i,j]) else '' for i in df.index]

        return df
    
    df = df.copy() # not to change initial df
    Teams, Players = class_args[:2]
    source, ma_num, folder = class_args[5:]
    
    if (name != 'TableTeams')&(len(xdf) > 0):
        #F_UF - is the dataframe of finished, unfinished or nonexiestent fixtures
        # 1- finished, 0 - unfinished, np.nan - doesn't exist
        F_UF = pd.DataFrame()
        for i in df.columns:
            F_UF[i]=[np.nan if np.isnan(xdf.at[j,i])&np.isnan(df.at[j,i]) else 1 for j in df.index]
            F_UF[i] = [np.nan if np.isnan(F_UF.at[j,i]) else 1 if np.isnan(xdf.at[j,i]) else 0 for j in df.index]
            
        if 'Team' in name:
            X = Teams.copy()
            X.columns = ['id', 'Teams', key_col, 'Matches']
            X[key_col] = df.mean(axis=1)
        else:
            X = Players.copy()
            n = Players.columns.get_loc('Team games')
            X.insert( n, key_col, df.mean(axis=1))
            X.insert(n + 1, key_col.replace('fixture','game'),X[key_col]*Players['Team games']/noZ(Players['Played']))
            
        df = GW_forecast(df, xdf, df_n, xdf_n)
        
        Y = df[df.columns[::-1]]
        X[Y.columns] = Y.copy()
        df = X.copy()
        del_empty_col(df)
    else:
        F_UF = df # Doen't matter. Just some dataFrame. Not really used
    
    #Removes redundant columns
#     del df['id']
    if 'id' in df.columns:
        del df['id']
    if 'Team number' in df.columns:
        del df['Team number']
    
    F_UF[key_col] = df.copy()[key_col]
    # Sort decreasing for attack an increasing for defence
    if 'Opponent' in name:
        df.sort_values(key_col, ascending = True, inplace = True)
        F_UF.sort_values(key_col, ascending = True, inplace = True)
    else:
        df.sort_values(key_col, ascending = False, inplace = True)
        F_UF.sort_values(key_col, ascending = False, inplace = True)
    
    df.index = np.arange(1, len(df) + 1)
    F_UF.index = np.arange(1, len(df) + 1)
    
    # If moving average number is 0 no calculation needed. Use it to fasten the debug!
    if ma_num == 0:
        df_ma = df
    else:
        df_ma = MA(df, ma_num)
    
    #Writes result to file
    df.to_csv(Path(f'{folder}out/{source}/{name}.csv'))
    
    
    # Add table created as table of stringe to existing html file replacing table tag with a new one
    #Also adding a new css
    if folder == '':
        dfString = table2string(df)
        
        if len(xdf) > 0:
            dfStringStyler = dfString.style.apply(color_table, axis=None, F_UF=F_UF)
        else:
            dfStringStyler = dfString.style
        dfStringStyler.set_table_attributes('class="DataTable sortable"')
        
        
        
        html_table = dfStringStyler.render().replace('\n', '')
        #html_table = dfStringStyler.to_html().replace('\n', '')
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

#         css = ('#' + source + '-button {\n background-color: #82f5cf;\n}\n' +
#                '#' + name + ' {\n background-color: lightgrey;\n}\n' + 
#                css)

        #Rewriting html and css
        with open(f'{folder}index.html', 'r', encoding="utf-8") as file:
            old_file = file.read()
        tag_to_replace1 = str(BeautifulSoup(old_file, 'html.parser')('table', attrs={"class":"DataTable"})[0])
        tag_to_replace2 = str(BeautifulSoup(old_file, 'html.parser')('h2', attrs={"id":"DataTitle"})[0])
        new_file = old_file.replace(tag_to_replace1, html_)
        new_file = new_file.replace(tag_to_replace2, f'<h2 id="DataTitle"> {html_table_name(name)} </h2>')

        #Making links right

#         if not(source=='FPL' and name=='TableTeams'):
#             new_file = new_file.replace('html/FPL/', '')
#             new_file = new_file.replace('"out/FPL/TableTeams.csv"', f'"../../out/{source}/{name}.csv"')#Download button
#             new_file = new_file.replace(f'TableTeams', name)
#             if name != 'TableTeams': #changes links for buttons FPL and Understat
#                 str1 = 'class="header-form" action="index.html"'
#                 str2 = f'class="header-form" action="../FPL/{name}.html"'
#                 new_file = new_file.replace(str1, str2)
#             else:
#                 new_file = new_file.replace('form action="index.html"', f'form action="../../index.html"')
#             new_file = new_file.replace('html/', '../')
#             new_file = new_file.replace('"index.html"', '"../../index.html"')

        #Creating .html and special files for the Table
        with open(html_path, 'w', encoding="utf-8") as file:
            file.write(new_file)
        with open(css_path, 'w', encoding="utf-8") as file:
            file.write(css)
    
    return df

def color_table(df, F_UF):
    '''
        Makes colored tables
    '''
    def table_type(df):
        '''
            Detecting the type of the table (TableTeams, Defence, Team, Player)
        '''
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
    
    def color_xTeam_xG(val):
        if val=='': color = 'white'
        elif float(val)>200: color = '#09bb9f'
        elif float(val)>140: color = '#82f5cf'
        elif float(val)>120: color = '#c4c4c4'
        else: color = '#15607a'
        return f'background: repeating-linear-gradient( 45deg, {color}, {color} 10px, white 10px, white 20px);'
    def color_xPlayer_xG(val):
        if val=='': color = 'white'
        elif float(val)>50: color = '#09bb9f'
        elif float(val)>25: color = '#82f5cf'
        elif float(val)>10: color = '#c4c4c4'
        else: color = '#15607a'
        return f'background: repeating-linear-gradient( 45deg, {color}, {color} 10px, white 10px, white 20px);'
    def color_xDefence(val):
        if val=='': color = 'white'
        elif float(val)<120: color = '#09bb9f'
        elif float(val)<150: color = '#82f5cf'
        elif float(val)<180: color = '#c4c4c4'
        else: color = '#15607a'
        return f'background: repeating-linear-gradient( 45deg, {color}, {color} 10px, white 10px, white 20px);'
    
    def color_table_row(row, x, F_UF):
        '''
            function for style.apply by rows where x is a table type
            Subfunction of color_table
        '''
        str1 = 'Background-color: white'
        #Column names which shouldn't be colored
        tw = (row.name in ['Matches', 'Teams'])#for teams
        pw = (row.name  in ['Team games', 'Played', 'Name', 'Team', 'Position'])#for players
        ttw = (row.name in ['xG adjusted', 'xG', 'xA', 'xA adjusted'])#for team defence
        
        if x=='Team':
            res = []
            for i in row.index:
                if tw: res.append(str1)
                elif F_UF.at[i, row.name]==0: res.append(color_xTeam_xG(row[i]))
                else: res.append(color_Team_xG(row[i]))
            return res
                
#             return [str1 if tw else \
#             color_Team_xG(row[i]) if F_UF.at[i, row.name]==1 else color_xTeam_xG(row[i]) for i in row.index]
        elif x=='Player':
            res = []
            for i in row.index:
                if pw: res.append(str1)
                elif not (row.name in F_UF.columns): res.append(color_Player_xG(row[i]))
                elif F_UF.at[i, row.name]==0: res.append(color_xPlayer_xG(row[i]))
                else: res.append(color_Player_xG(row[i]))
            return res
        
#             return [str1 if pw else color_Player_xG(row[i]) if not (row.name in F_UF.columns) else\
#             color_Player_xG(row[i]) if F_UF.at[i, row.name]==1 else color_xPlayer_xG(row[i]) for i in row.index]
        elif x=='Defence':
            res = []
            for i in row.index:
                if tw: res.append(str1)
                elif F_UF.at[i, row.name]==0: res.append(color_xDefence(row[i]))
                else: res.append(color_Defence(row[i]))
            return res
#             return [str1 if tw else \
#             color_Defence(row[i]) if F_UF.at[i, row.name]==1 else color_xDefence(row[i]) for i in row.index]
        elif x=='TableTeams':
            return [color_Team_xG(i) if ttw else str1 if row.name in ['Team'] else color_Defence(i) for i in row]
        
    return df.apply(color_table_row, args=(table_type(df), F_UF), axis=0)

def MA(Out_T, d):
    '''
        Returns the table with d mean average for table Out_T.
        If less than d matches played returns zero. If no match played in gameweeek previous gameweek taken.
        Filling the column with averages. Subfunction for MA.
        Adds d - averages for j-th GW_column (not GWj but j-th in order) of table T.
    '''
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
    
    
#     return dfAd
def adjustment(name, df, class_args):
    '''
        Creating adjusted tables from unadjusted
        name - table name before transformation
        df - table to transform
        class_args - list of needed data from source class
    '''
    [Teams, Players, Team_Attack_weight_unadj, Team_Defence_weight_unadj, Player_Attack_weight_unadj] = class_args[:5]
    
    Name = name.split('_')
    
    key_par = ' '.join(Name[1:])
    if Name[0] == 'Team':
        dfAdjusted = Teams.copy()
        dfAdjusted.columns = ['id', 'Teams', f'{key_par} av adj', 'Matches']
        if key_par in {'xG', 'xA'}:
            weight = Team_Attack_weight_unadj
        else:
            weight = Team_Defence_weight_unadj
    else:
        dfAdjusted = Players.copy()
        weight = Player_Attack_weight_unadj
    
#     print(name)
#     print(df.columns)
#     print(weight.columns)
    dfAd = df/weight #Main formula
    #print(dfAd.columns)
    dfAd = dfAd[list(df.columns)]
    
    X = dfAd[list(df.columns[::-1])]
    dfAdjusted[X.columns] = X
    del_empty_col(dfAdjusted)
#     print(dfAdjusted.columns)
    #print(dfAdjusted)
    
    if Name[0] == 'Team':
        dfAdjusted[f'{key_par} av adj'] = dfAd.mean(axis=1)
    else:
        n = Players.columns.get_loc('Team games')
        dfAdjusted.insert(n, f'{key_par} per fixture adj', dfAd.sum(axis=1)/noZ(Players['Team games']))
        a = dfAdjusted[f'{key_par} per fixture adj']
        dfAdjusted.insert(n+1, f'{key_par} per game adj', a*Players['Team games']/noZ(Players['Played']))
    
#     print(dfAd.columns)
#     print(dfAdjusted.columns)
    return dfAd, dfAdjusted
        
if __name__ == '__main__':
    year = ''
    Understat = Source('Understat', ma_num=7, year=year)
    Understat.test(year)
    FPL = Source('FPL', ma_num=7, year=year)
    FPL.test(year)
    #display(MA(Understat.TeamThreatAd, 8))
    print('End')
    pass