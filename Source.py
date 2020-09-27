#!/usr/bin/env python
# coding: utf-8

# In[1]:


from time import time
import constti
from Brr_functions import no_lists, toint, noZ
from bs4 import BeautifulSoup
import requests
import pandas as pd
import json
import numpy as np
from pathlib import Path
import os

class Source:
    def __init__(self, source, ma_num=0):
        self.source = source
        print(f'Start Creating {self.source} Tables:')
        start_module = time()
        start = time()
        self.ma_num = ma_num # Number of matches for the moving average
        
#1. Reading nessesry data from files.
        
        try:
            Table = pd.read_csv(Path('in/Table_'+source+'.csv')) # Main data from source containing rows for each player
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
        Fixtures = pd.read_csv(Path('in/Fixtures.csv')) # Table of rows for each fixture
        Teams = pd.read_csv(Path('in/Teams.csv')) # Table of rows as teams with columns 'id', 'Teams',
        #'TARGET COL', 'Matches'
        Players = pd.read_csv(Path('in/Players.csv')) # Table of rows as players with columns 'id', 'Name',
        #'Team', Team games', 'Played'
        del Players['web_name'] #This column is needed only for inputUnderstat.py
        
        #Reading Fxtures and Opponents for Teams and Players
        with open('in/Team_fixtures.txt', 'r') as file:
            Team_fixtures = pd.DataFrame(json.loads(file.read()))
            Team_fixtures.index = pd.to_numeric(Team_fixtures.index)
            Team_fixtures = Team_fixtures.sort_index()
        with open('in/Team_opponent_team.txt', 'r') as file:
            Team_opponent_team = pd.DataFrame(json.loads(file.read()))
            Team_opponent_team.index = pd.to_numeric(Team_opponent_team.index)
            Team_opponent_team = Team_opponent_team.sort_index()
        with open('in/Player_fixtures.txt', 'r') as file:
            Player_fixtures = pd.DataFrame(json.loads(file.read()))
            Player_fixtures.index = pd.to_numeric(Player_fixtures.index)
            Player_fixtures = Player_fixtures.sort_index()
        with open('in/Player_opponent_team.txt', 'r') as file:
            Player_opponent_team = pd.DataFrame(json.loads(file.read()))
            Player_opponent_team.index = pd.to_numeric(Player_opponent_team.index)
            Player_opponent_team = Player_opponent_team.sort_index()
        
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

        TeamThreat = Teams.copy()
        TeamThreat.columns = ['id', 'Teams', 'Threat av', 'Matches']

        for j in range(lastGW,0,-1):
            TeamThreat['Threat GW'+str(j)] = [[] for _ in range(team_number)]
            for i in range(team_number):
                for k in range(len(Team_fixtures.at[i, 'GW'+str(j)])):
                    TeamThreat.at[i,'Threat GW'+str(j)].append(Table[(Table['fixture']==                                        Team_fixtures.at[i, 'GW'+str(j)][k])&(Table['team']==i+1)]['threat'].sum())


        TeamThreat['Threat av'] = [Table[Table['team']==i]['threat'].sum() for i in range(1,team_number+1)]             /noZ(Teams['Matches'])
        TeamThreat.sort_values('Threat av', ascending = False, inplace = True)

        print('\t\t 3.1. TeamThreat is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(2). Creating  a table with average creativity and GW creativities for teams

        TeamCreativity = Teams.copy()
        TeamCreativity.columns = ['id', 'Teams', 'Creativity av', 'Matches']

        for j in range(lastGW,0,-1):
            TeamCreativity['Creativity GW'+str(j)] = [[] for _ in range(team_number)]
            for i in range(team_number):
                for k in range(len(Team_fixtures.at[i, 'GW'+str(j)])):
                    TeamCreativity.at[i,'Creativity GW'+str(j)].append(Table[(Table['fixture']==                                    Team_fixtures.at[i, 'GW'+str(j)][k])&(Table['team']==i+1)]['creativity'].sum())


        TeamCreativity['Creativity av'] = [Table[Table['team']==i]['creativity'].sum() for i in range(1,team_number+1)]             /noZ(Teams['Matches'])
        
        print('\t\t 3.2. TeamCreativity is over.\t It takes ' + str(time() - start)+ ' sec')
        start = time()
        
        #(3). Creating  a table with average threat allowed by teams and GW threat allowed

        TeamDefence = Teams.copy()
        TeamDefence.columns = ['id', 'Teams', 'Threat allowed av', 'Matches']

        for j in range(lastGW,0,-1):
            TeamDefence['Threat allowed GW'+str(j)] = [[] for _ in range(team_number)]
            for i in range(team_number):
                for k in range(len(Team_fixtures.at[i, 'GW'+str(j)])):
                    TeamDefence.at[i,'Threat allowed GW'+str(j)].append(Table[(Table['fixture']==                         Team_fixtures.at[i, 'GW'+str(j)][k])&(Table['opponent_team']==i+1)]['threat'].sum())


        TeamDefence['Threat allowed av'] = [Table[Table['opponent_team']==i]['threat'].sum()
                        for i in range(1,team_number+1)]/noZ(Teams['Matches'])

        threatAllowedAv = TeamDefence['Threat allowed av'].mean()
        
        print('\t\t 3.3. TeamDefence is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        class_data = [Teams, Players, Team_fixtures, Player_fixtures, Team_opponent_team,        Player_opponent_team, TeamThreat, TeamDefence, threatAllowedAv, lastGW]
        #(4). Creating  a table with average adjusted threat and GW threats adj for teams

        TeamThreatAd = adjustment(TeamThreat, class_data)       
        print('\t\t 3.4. TeamThreatAd is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(5). Creating  a table with average adjusted creativity and GW creativities adj for teams
        
        TeamCreativityAd = adjustment(TeamCreativity, class_data)
        print('\t\t 3.5. TeamCreativityAd is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(6). Creating  a table with average threat allowed adjusted by teams and GW threat allowed adjusted

        TeamDefenceAd = adjustment(TeamDefence, class_data)
        print('\t\t 3.6. TeamDefenceAd is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()


        #(7) Total Team Table

        TableTeams = pd.DataFrame()
        TableTeams['id'] = Teams['id']
        TableTeams['Team'] = Teams['Teams']

        TableTeams['Threat adjusted'] = TeamThreatAd['Threat av adj']
        TableTeams['Threat'] = TeamThreat['Threat av']
        TableTeams['Creativity adjusted'] = TeamCreativityAd['Creativity av adj']
        TableTeams['Creativity'] = TeamCreativity['Creativity av']
        TableTeams['Threat allowed adjusted'] = TeamDefenceAd['Threat allowed av adj']
        TableTeams['Threat allowed'] = TeamDefence['Threat allowed av']
        
        print('\t\t 3.7. TableTeams is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()

# 4. Calculating pLayer tables
        
        print('\t 4. Calculating player tables')

        #(1) Players Threat

        PlayerThreat = Players.copy()
        #PlayerThreat['Threat per fixture'] = np.zeros(len(Players))
        #PlayerThreat['Threat per game'] = np.zeros(len(Players))
        PlayerThreat.insert(PlayerThreat.columns.get_loc('Team games'), 'Threat per fixture',        [0.0 for i in PlayerThreat.itertuples()])
        PlayerThreat.insert(PlayerThreat.columns.get_loc('Team games'), 'Threat per game',        [0.0 for i in PlayerThreat.itertuples()])

        for j in range(lastGW,0,-1):
            PlayerThreat['Threat GW'+str(j)] = [[] for _ in range(len(Players))]
            for i in range(len(Players)):
                for k in range(len(Player_fixtures.at[i, 'GW'+str(j)])):
                    PlayerThreat.at[i,'Threat GW'+str(j)].append(Table[(Table['fixture']==                        Player_fixtures.at[i, 'GW'+str(j)][k])&(Table['element']==                        PlayerThreat.at[i,'id'])]['threat'].sum())
                    PlayerThreat.at[i,'Threat per game'] = PlayerThreat.at[i,'Threat per game'] +                        PlayerThreat.at[i,'Threat GW'+str(j)][k]


        PlayerThreat['Threat per fixture'] = PlayerThreat['Threat per game']/noZ(Players['Team games'])
        PlayerThreat['Threat per game'] = PlayerThreat['Threat per game']/noZ(Players['Played'])
        
        print('\t\t 4.1. PlayerThreat is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(2) Players Creativity

        PlayerCreativity = Players.copy()
        #PlayerCreativity['Creativity per fixture'] = np.zeros(len(Players))
        #PlayerCreativity['Creativity per game'] = np.zeros(len(Players))
        PlayerCreativity.insert(PlayerCreativity.columns.get_loc('Team games'), 'Creativity per fixture',        [0.0 for i in PlayerCreativity.itertuples()])
        PlayerCreativity.insert(PlayerCreativity.columns.get_loc('Team games'), 'Creativity per game',        [0.0 for i in PlayerCreativity.itertuples()])

        for j in range(lastGW,0,-1):
            PlayerCreativity['Creativity GW'+str(j)] = [[] for _ in range(len(Players))]
            for i in range(len(Players)):
                for k in range(len(Player_fixtures.at[i, 'GW'+str(j)])):
                    PlayerCreativity.at[i,'Creativity GW'+str(j)].append(Table[(Table['fixture']==                                                Player_fixtures.at[i, 'GW'+str(j)][k])&                                                (Table['element']==PlayerCreativity.at[i,'id'])]['creativity'].sum())
                    PlayerCreativity.at[i,'Creativity per game'] = PlayerCreativity.at[i,'Creativity per game'] +                        PlayerCreativity.at[i,'Creativity GW'+str(j)][k]


        PlayerCreativity['Creativity per fixture'] = PlayerCreativity['Creativity per game']/noZ(Players['Team games'])
        PlayerCreativity['Creativity per game'] = PlayerCreativity['Creativity per game']/noZ(Players['Played'])
        
        print('\t\t 4.2. PlayerCreativity is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()
        
        #(3) Players Threat Adjusted
        
        PlayerThreatAd = adjustment(PlayerThreat, class_data)
        print('\t\t 4.3. PlayerThreatAd is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()

        #(4) PLayers Creativity Adjusted
        
        PlayerCreativityAd = adjustment(PlayerCreativity, class_data)    
        print('\t\t 4.4. PlayerCreativityAd is over.\t It takes ' + str(time() - start) + ' sec')
        start = time()

# 5. Writing tables to files (and variables to class.variables)
        
        # Useful variables for debugging
        self.Table = Table
        self.Fixtures = Fixtures
        self.Teams = Teams
        self.Players = Players
        
        self.threatAllowedAv = threatAllowedAv
        self.Team_fixtures = Team_fixtures
        self.Team_opponent_team = Team_opponent_team
        self.Player_fixtures = Player_fixtures
        self.Player_opponent_team = Player_opponent_team
        
        #Save before modifying while riting into files
        self.TT = TeamThreat.copy()
        self.TC = TeamCreativity.copy()
        self.TD = TeamDefence.copy()
        self.TTA = TeamThreatAd.copy()
        self.TCA = TeamCreativityAd.copy()
        self.TDA = TeamDefenceAd.copy()
        self.TaTe = TableTeams.copy() 
        self.PT = PlayerThreat.copy()
        self.PC = PlayerCreativity.copy()
        self.PTA = PlayerThreatAd.copy()
        self.PCA = PlayerCreativityAd.copy()
        
        self.TeamThreat =            write_table(TeamThreat, 'TeamThreat', 'Threat av', self.source, self.ma_num)
        self.TeamCreativity = (write_table
            (TeamCreativity, 'TeamCreativity', 'Creativity av', self.source, self.ma_num))
        self.TeamDefence =            write_table(TeamDefence, 'TeamDefence', 'Threat allowed av', self.source, self.ma_num)
        self.TeamThreatAd =            write_table(TeamThreatAd, 'TeamThreatAd', 'Threat av adj', self.source, self.ma_num)
        self.TeamCreativityAd =            write_table(TeamCreativityAd, 'TeamCreativityAd', 'Creativity av adj', self.source, self.ma_num)
        self.TeamDefenceAd =            write_table(TeamDefenceAd, 'TeamDefenceAd', 'Threat allowed av adj', self.source, self.ma_num)
        self.TableTeams =            write_table(TableTeams, 'TableTeams', 'Threat adjusted', self.source, self.ma_num)
        
        self.PlayerThreat =            write_table(PlayerThreat, 'PlayerThreat', 'Threat per fixture', self.source, self.ma_num)
        self.PlayerCreativity =            write_table(PlayerCreativity, 'PlayerCreativity', 'Creativity per fixture', self.source, self.ma_num)
        self.PlayerThreatAd =            write_table(PlayerThreatAd, 'PlayerThreatAd', 'Threat per fixture adj', self.source, self.ma_num)
        self.PlayerCreativityAd =            write_table(PlayerCreativityAd, 'PlayerCreativityAd', 'Creativity per fixture adj', self.source, self.ma_num)
        
        print('\t 5. Writing to files is over.\t It takes ' + str(time() - start) + ' sec')
        print(f'{self.source} is created./t It takes {time() - start_module} sec\n')
        start = time()

# 6. Tests for each element of the class
    
    # Checks if all names in the source can be found in fantasy.premierleague.com (FPL)
    def test2FPL(self):
        FPL = pd.read_csv('in/Table_FPL.csv')
        Table_Source = pd.read_csv('in/Table_'+self.source+'.csv')
        Mistakes = pd.DataFrame()
        No_Names = pd.DataFrame()
        if self.source == 'FPL':
            return Mistakes, No_Names
        for i in Table_Source.index:
            if Table_Source.at[i,'element'] < 1000000:
                FPLmin = FPL[(FPL['element'] == Table_Source.at[i,'element'])&                            (FPL['fixture'] == Table_Source.at[i,'fixture'])]['minutes'].sum()
                if abs(Table_Source.at[i,'minutes'] - FPLmin) > 10:
                    Mistakes = Mistakes.append(pd.DataFrame([[Table_Source.at[i,'player'], Table_Source.at[i,'element'],                        Table_Source.at[i,'fixture'], Table_Source.at[i,'minutes'], FPLmin]], columns = ['player',                        'element', 'fixture', 'minutes', 'FPL_minutes']), ignore_index=True)
            else:
                No_Names = No_Names.append(pd.DataFrame([[Table_Source.at[i,'player'], Table_Source.at[i,'team_name'], Table_Source.at[i,'fixture']]],                                                         columns = ['player', 'team_name', 'fixture']), ignore_index=True)
                #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        display(Mistakes)
        display(No_Names)
        return Mistakes, No_Names
        
    def test(self):
        if self.Table.empty:
            print(f'Nothing to test. {self.source} table is empty.')
            return
        start = time()
        
        constti.DRDC(self.TeamThreat)
        constti.DRDC(self.TeamCreativity)
        constti.DRDC(self.TeamDefence)
        constti.DRDC(self.TeamThreatAd)
        constti.DRDC(self.TeamCreativityAd)
        constti.DRDC(self.TeamDefenceAd)
        constti.DRDC(self.TableTeams)
        constti.DRDC(self.PlayerThreat)
        constti.DRDC(self.PlayerCreativity)
        constti.DRDC(self.PlayerThreatAd)
        constti.DRDC(self.PlayerCreativityAd)
        
        self.test2FPL()
        
        print(f'\t {self.source} Testing is over.\t It takes {time() - start} sec')

# 7. Useful not class functions

def html_name(table_name):
    '''
    Converts the name of the table into the name of the html file
    '''
    return (table_name.replace('Threat', '_Attack').replace('Defence', '_Defence')
            .replace('Creativity', '_Creativity').replace('Ad','_Ad'))

# Writing final tables to files and returning table itself and MA variant also
# df - Table to make final table out of it, name - the name of the table, key_col - column to sort,
# source - Understat or FPL, ma_num - number for MA(currently unused just calculated)
def write_table(df, name, key_col, source, ma_num):
    
    #Removes redundant columns
    del df['id']
    if 'Team number' in df.columns:
        del df['Team number']
    
    # Sort decreasing for attack an increasing for defence
    if 'Defence' in name:
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
    df.to_csv(Path('out/' + source + '/' + name + '.csv'))
    
    
    # Add table to existing html file replacing table tag with a new one
    #Also adding a new css
    html_table = df.style.apply(color_table, axis=None).render().replace('\n', '')
    BS_table = BeautifulSoup(html_table, 'html.parser')
    css = str(BS_table('style')[0])
    html_ = str(BS_table('table')[0])
    
    #Getting right paths for different tables
    if source=='FPL' and name=='TableTeams':
        html_path = Path('index.html')
        css_path = Path(f'html/fpl/css/TableTeams.css')
    elif name=='TableTeams':
        html_path = Path('html/uns/main.html')
        css_path = Path(f'html/uns/css/TableTeams.css')
    elif source=='FPL':
        html_path = Path(f'html/{source.lower()}/{html_name(name)}.html')
        css_path = Path(f'html/{source.lower()}/css/{html_name(name)}.css')
    else:
        html_path = Path(f'html/uns/{html_name(name)}.html')
        css_path = Path(f'html/uns/css/{html_name(name)}.css')
        
    #Rewriting html and css
    with open(html_path, 'r', encoding="utf-8") as file:
        old_file = file.read()
    tag_to_replace = str(BeautifulSoup(old_file, 'html.parser')('table')[0])
    new_file = old_file.replace(tag_to_replace, html_)
    with open(html_path, 'w', encoding="utf-8") as file:
        file.write(new_file)
    with open(Path('html/FPLstyle.css'), 'r', encoding="utf-8") as file:
        css_template = file.read()
    with open(css_path, 'w', encoding="utf-8") as file:
        file.write(css_template+css)
    
    return df


#Makes colored tables
def color_table(df):
    def table_type(df):
        if df.columns[2]=='Threat': return 'TableTeams'
        elif 'allowed' in df.columns[1]: return 'Defence'
        elif df.columns[0]=='Teams': return 'Team'
        else: return 'Player'
    def color_TeamThreat(val):
        if type(val)==str: color = 'white'
        elif val>200: color = '#09bb9f'
        elif val>140: color = '#82f5cf'
        elif val>120: color = '#c4c4c4'
        else: color = '#15607a'
        return f'Background-color: {color}'
    def color_PlayerThreat(val):
        if type(val)==str: color = 'white'
        elif val>50: color = '#09bb9f'
        elif val>25: color = '#82f5cf'
        elif val>10: color = '#c4c4c4'
        else: color = '#15607a'
        return f'Background-color: {color}'
    def color_Defence(val):
        if type(val)==str: color = 'white'
        elif val<120: color = '#09bb9f'
        elif val<150: color = '#82f5cf'
        elif val<180: color = '#c4c4c4'
        else: color = '#15607a'
        return f'Background-color: {color}'
    
    #function for style.apply by rows where x is a table type
    def color_table_row(row, x):
        if x=='Team':
            return [color_TeamThreat(i) if row.name != 'Matches' else 'Background-color: white' for i in row]
        elif x=='Player':
            return [color_PlayerThreat(i) if not (row.name  in ['Team games', 'Played'])                    else 'Background-color: white' for i in row]
        elif x=='Defence':
            return [color_Defence(i) if row.name != 'Matches' else 'Background-color: white' for i in row]
        elif x=='TableTeams':
            return [color_TeamThreat(i) if row.name in ['Threat adjusted', 'Threat', 'Creativity', 'Creativity adjusted']                   else color_Defence(i) for i in row]
        
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
                if T.at[i, GW_columns[j-k]] != '':
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
# TeamDefence = Understat.TD
# Team_fixtures = Understat.Team_fixtures
# Team_opponent_team = Understat.Team_opponent_team
# Player_opponent_team = Understat.Player_opponent_team
# Player_fixtures = Understat.Player_fixtures
# TeamThreat = Understat.TT
# PlayerCreativity = Understat.PC






def adjustment(df, class_data):
    [Teams, Players, Team_fixtures, Player_fixtures, Team_opponent_team, Player_opponent_team,     TeamThreat, TeamDefence, threatAllowedAv, lastGW]    = class_data
    if len(df)==len(Teams):
        dfAd = Teams.copy()
        for i in df.columns:
            if ' av' in i:
                key_par = i[:-3]
        av  = 'av'
        dfAd.columns = ['id', 'Teams', f'{key_par} av adj', 'Matches']
        the_fixtures = Team_fixtures
        the_opponent_team = Team_opponent_team
    else:
        dfAd = Players.copy()
        for i in df.columns:
            if 'per fixture' in i:
                key_par = i[:-12]
        av = 'per game'
        
        dfAd.insert(dfAd.columns.get_loc('Team games'), f'{key_par} per fixture adj',        [0.0 for i in dfAd.itertuples()])
        dfAd.insert(dfAd.columns.get_loc('Team games'), f'{key_par} per game adj',        [0.0 for i in dfAd.itertuples()])
        
        
        the_fixtures = Player_fixtures
        the_opponent_team = Player_opponent_team
    if key_par[-7:] == 'allowed':
        Weighting_table = TeamThreat
        col = 'Threat av'
    else:
        Weighting_table = TeamDefence
        col = 'Threat allowed av'
        
    for j in range(lastGW,0,-1):
        dfAd[f'{key_par} GW{j} adj'] = [[] for _ in range(len(df))]
        for i in range(len(df)):
             for k in range(len(the_fixtures.at[i, 'GW'+str(j)])):
                dfAd.at[i,f'{key_par} GW{j} adj'].append(df.at[i,f'{key_par} GW{j}'][k]                *threatAllowedAv/ Weighting_table.at[the_opponent_team.at[i,f'GW{j}'][k]-1, col])

                dfAd.at[i,f'{key_par} {av} adj'] += dfAd.at[i,f'{key_par} GW{j} adj'][k]

    if len(df) == len(Teams): dfAd[f'{key_par} av adj'] = dfAd[f'{key_par} av adj']/noZ(dfAd['Matches'])
    else:
        dfAd[f'{key_par} per fixture adj'] = dfAd[f'{key_par} per game adj']/noZ(dfAd['Team games'])
        dfAd[f'{key_par} per game adj'] = dfAd[f'{key_par} per game adj']/noZ(dfAd['Played'])
    
    
    return dfAd

        
if __name__ == '__main__':
    Understat = Source('Understat', ma_num=7)
    Understat.test()
    FPL = Source('FPL', ma_num=7)
    FPL.test()
    #display(MA(Understat.TeamThreatAd, 8))
    pass


# In[ ]:




