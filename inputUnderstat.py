''' Understat Download
    
    Downloads data from https://understat.com/ and calculates basic pandas tables.
    
    Sources:    Understat (no API)
                'in/LTable_FPL.csv'
                'in/Fixtures.csv'
                'in/Teams.csv'
                'in/Players.csv'
                
    Writes:     'in/Table_Understat.csv'
                'in/Name_Dictionary.csv'
                
'''


#Downloadting Data from understat.com(Understat)
print('Start inputUnderstat:')
from time import time
start_module = time()
start = start_module

from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import numpy as np
import codecs
from pathlib import Path
import constti

# 1.Check if there are equal names in the column
def  check_equal(Table, col_name): #Check doubles in column col_name of the table Table
    '''
    Check if there are equal names in the column
    '''
    repetitions = pd.DataFrame()
    for i in range(len(Table)):
        if Table[col_name][i] in set(Table[col_name][:i]):
            if (len(repetitions)>0):
                if not (Table[col_name][i] in set(repetitions[col_name])):
                    repetitions = repetitions.append(Table[Table[col_name]==Table[col_name][i]])
            else:
                repetitions = repetitions.append(Table[Table[col_name]==Table[col_name][i]])
            #print('Check Equal', Table.iloc[i])
    print(repetitions)

# 2. Check if name_un is the same player as (name_fpl, web_name_fpl). Five comparison types: the lower the better
def name2standart(name):
    '''
    Changes name to the same standarts:
        All characters are low.
        Correct apostrof
        Odegaard = Ødegaard
    '''
    return name.lower().replace('ø', 'o').replace("&#039;", "'")

def same_player(name_un, name_fpl, web_name_fpl, type_comp, repeat=1):
    '''
    Check if name_un is the same player as (name_fpl, web_name_fpl). Five comparison types: the lower the better
    '''
    name_un = name2standart(name_un)#name_un.lower()
    name_fpl = name2standart(name_fpl)#name_fpl.lower()
    web_name_fpl = name2standart(web_name_fpl)#web_name_fpl.lower()
    same = 0
    
    # Unerstat name = Full FPL name. Best equality
    if (type_comp == 0)&(name_un == name_fpl):
        same = 1
    
    # Understat name = Short FPL name.
    if (type_comp == 1)&(web_name_fpl == name_un):
        same = 1
    
    # Understat name is included in Full FPL name.
    if (type_comp == 2)&(name_un in name_fpl):
        same = 1
    
    # All words of Understat name are in Full FPL name
    if (type_comp == 3)&(set(name_un.split()) <= set(name_fpl.split())):
        same = 1
    
    # FPL Short name is included in Understat name
    if (type_comp == 4)&(web_name_fpl in name_un):
        same = 1
    
    # Some ' ' space has been missed and after del of one space all words of Understat name are in Full FPL
    if type_comp == 5:
        fpl_name = name_fpl.lower().split()
        for m in range(1,len(fpl_name)):
            if set(name_un.lower().split()) <=\
            set([fpl_name[k-1]+fpl_name[k] if k==m else ('' if k==m-1 else fpl_name[k])\
                 for k in range(0,len(fpl_name))]):
                    same = 1
                    
    # If '-' in Understat is ' ' in FPL    DO IT ALL AGAIN!!   but one time
    if (same == 0)&(repeat==1):
        return same_player(name_un.replace('-', ' '), name_fpl, web_name_fpl, type_comp, 0)
    return same

# 3. Downloads match data, adds match number game_number to Table and updates Dictionary of names
def add_match_to_dict(game_number, Dictionary):
    '''
    Downloads match data, adds match number game_number to Table and updates Dictionary of names
    '''
    url = 'https://understat.com/match/'+ str(game_number)
    #print(url)
    p = constti.long_request(url)
    pdecoded = codecs.decode(p.text,'unicode_escape')
    page = BeautifulSoup(p.text, 'html.parser')
    tempList = []
    for tags in page('script'):
        if '= JSON.parse' in str(tags):
            for els in str(tags).split():
                if 'JSON' in els:
                    els = els[12:-3]
                    els = codecs.decode(els,'unicode_escape')
                    tempList.append(json.loads(els))
    
    away_players = pd.DataFrame(tempList[2]['a']).transpose()
    home_players = pd.DataFrame(tempList[2]['h']).transpose()
    match_players = away_players.append(home_players)
    
    if tempList[1]['team_h'] == 'Tottenham': tempList[1]['team_h'] = 'Spurs' 
    if tempList[1]['team_a'] == 'Tottenham': tempList[1]['team_a'] = 'Spurs'
    
    h = list([teams_dict[tempList[1]['team_h']] for i in range(len(match_players))])
    a = list([teams_dict[tempList[1]['team_a']] for i in range(len(match_players))])
    ha = list(match_players['h_a'])
    di = dict(zip(Teams['Teams'], Teams['id']))
    
    match_players['team_h_name'] = h
    match_players['team_a_name'] = a   
    match_players['team_name'] = [a[i] if ha[i] == 'a' else h[i] for i in range(len(a))]
    match_players['opponent_team_name'] = [a[i] if ha[i] == 'h' else h[i] for i in range(len(a))]
    match_players['team_h'] = [di[h[i]] for i in range(len(a))]
    match_players['team_a'] = [di[a[i]] for i in range(len(a))]
    match_players['team'] = [di[match_players.at[i,'team_name']] for i in match_players.index]
    match_players['opponent_team']=[di[match_players.at[i,'opponent_team_name']] for i in match_players.index]
    
    A = Fixtures['team_a']==match_players['team_a'].mean()
    B = Fixtures['team_h']==match_players['team_h'].mean()
    f = Fixtures[A&B]['id'].sum()
    
    match_players['fixture'] = [f for _ in range(len(a))]
    
    r = int(Fixtures[Fixtures['id']==match_players['fixture'].mean()]['event'].sum())
    
    match_players['round'] = [ r for _ in range(len(a))]
    match_players['player'] = constti.strip_accents_pdlist(pd.DataFrame(match_players['player']))
    
    sT = Table_FPL[Table_FPL['fixture']==match_players['fixture'].mean()]
    di = dict(zip(Players['id'], Players['Name']))
    A = [di[sT.at[i,'element']] for i in sT.index]
    
    
    FPL_names = constti.strip_accents_pdlist(pd.DataFrame(A, columns = ['player']))
    
    di = dict(zip(Players['id'], Players['web_name']))
    A = [di[sT.at[i,'element']] for i in sT.index]
    
    FPL_names['web_name'] = constti.strip_accents_pdlist(pd.DataFrame(A, columns = ['player']))
    FPL_names['id'] = [sT.at[i,'element'] for i in sT.index]
    match_players['in_FPL'] = [0 for i in match_players.index]
    
    Dictionary_strong = Dictionary[Dictionary['id_fpl']!='']

    for i in match_players.index:
        match_players.at[i,'player'] = match_players.at[i,'player']
        if not(match_players.at[i,'player'] in set(Dictionary_strong['name_un'])):
            
            for t in range(6):
                for j in FPL_names.index:
                    if same_player(match_players.at[i,'player'], FPL_names.at[j,'player'], FPL_names.at[j,'web_name'], t):
                        match_players.at[i,'in_FPL'] = 1
                        name_un = match_players.at[i, 'player']
                        name_fpl = FPL_names.at[j,'player']
                        id_fpl = FPL_names.at[j,'id']
                        web_name_fpl = FPL_names.at[j,'web_name']
                        match_players.at[i, 'player'] = FPL_names.at[j,'player']
                        break
                if match_players.at[i,'in_FPL'] == 1:
                    break
            if match_players.at[i,'in_FPL'] == 0:
                name_un = match_players.at[i, 'player']
                name_fpl = ''
                id_fpl = ''
                web_name_fpl = ''
                
                
                
            if name_un in set(Dictionary['name_un']):
                print('SMTH went wrong')
                if name_fpl != '':
                    for u in Dictionary.index:
                        if name_un == Dictionary.at[u,'name_un']:
                            Dictionary.at[u,'name_fpl'] = name_fpl
                            Dictionary.at[u,'id_fpl'] = id_fpl
                            Dictionary.at[u,'web_name_fpl'] = web_name_fpl
            else:

                Dictionary = Dictionary.append(pd.DataFrame(\
                    [[name_un, name_fpl, id_fpl, web_name_fpl]],\
                    columns=["name_un", 'name_fpl', 'id_fpl', 'web_name_fpl']), ignore_index=True)
        else:
            match_players.at[i, 'player'] = dict(zip(Dictionary['name_un'], Dictionary['name_fpl']))\
                [match_players.at[i, 'player']]
            match_players.at[i,'in_FPL'] = 1

    match_players = match_players.sort_index()
    Dictionary = Dictionary.sort_values('name_un')
    Dictionary.index = np.arange(1, len(Dictionary) + 1)
    return match_players, Dictionary

# 4. Adding exceptions to Name_Dictionary
def Exc_dict(Name_Dictionary, name_understat, name_fpl):
    '''
    Adding exceptions to Name_Dictionary
    name_understat - "player" column in Table_Understat
    name_fpl - "first_name" + " " + "second_name" in bootstrap table
    '''
    Name_Dictionary = Name_Dictionary.append(pd.DataFrame([[name_understat,name_fpl,'','']], 
    columns=["name_un", 'name_fpl', 'id_fpl', 'web_name_fpl']), ignore_index=True)
#     display(Name_Dictionary)
#     print("asdasd" + Name_Dictionary.at[len(Name_Dictionary)-1, 'id_fpl'])    
    Name_Dictionary.at[len(Name_Dictionary)-1, 'id_fpl'] = Players[Players['Name']==name_fpl]['id'].mean()
#     display(Players[Players['Name']==name_fpl]['web_name'])
    Name_Dictionary.at[len(Name_Dictionary)-1, 'web_name_fpl'] = Players[Players['Name']==name_fpl]['web_name'].iat[0]
    #print(Name_Dictionary)
    #print(name_understat)
    return Name_Dictionary



# class StopExecution(Exception):
#     def _render_traceback_(self):
#         pass



#Read data from fantasy.premierleague.com(FPL) to compare with
'''
    Large_Table is needed only for inputUndersat. To get FPL names when Understat data is already calculated
    but FPL data is not. So players played are not excluded from the table but have zero data.
'''
try:
    Table_FPL = pd.read_csv('in/LTable_FPL.csv') #Main table of FPL
    1/len(Table_FPL)
    table_len = len(Table_FPL)
except:
    print("Table_FPL is empty or can't be read")
    table_len = 0
    pd.DataFrame().to_csv(Path('in/Table_Understat.csv'), index=False)
    pd.DataFrame().to_csv(Path('in/Name_Dictionary.csv'), index=False)
#     raise StopExecution
    
if table_len > 0:
    Fixtures = pd.read_csv('in/Fixtures.csv') #All fixtures with postponed
    Teams = pd.read_csv('in/Teams.csv') #Team Tables Template
    Players = pd.read_csv('in/Players.csv') #Player Table Template 

    url = 'https://understat.com/match/11919' #match data by id
    url1 = 'https://understat.com/league/EPL' #url to get list of matches and their id

    #Getting matches id and 
    p = constti.long_request(url1)
    pdecoded = codecs.decode(p.text,'unicode_escape')
    page = BeautifulSoup(p.text, 'html.parser')
    a = []
    for tags in page('script'):
        if '= JSON.parse' in str(tags):
            for els in str(tags).split():
                if 'JSON' in els:
                    els = els[12:-3]
                    els = codecs.decode(els,'unicode_escape')
                    a.append(json.loads(els))
    TT = pd.DataFrame(a[1]).transpose()
    UnderstatTeams = dict(zip(TT['id'], TT['title']))
    Schedule = pd.DataFrame(a[0])

    #Словарь для перевода understat команд к FPL именам Name_Dictionary
    teams_dtable = pd.DataFrame()
    teams_dtable['understat'] = ['Spurs' if i == 'Tottenham' else i for i in TT.sort_values(by=['title'])['title']]
    teams_dtable.index = np.arange(0, len(teams_dtable))
    teams_dtable['fpl'] = list(Teams.sort_values(by=['Teams'])['Teams'])
    teams_dict = dict(zip(teams_dtable['understat'], teams_dtable['fpl']))

    #Downloads all match data
    Table_Understat = pd.DataFrame()
    Name_Dictionary = pd.DataFrame(columns=["name_un", 'name_fpl', 'id_fpl', 'web_name_fpl'])

    #Adding exceptions to Dictionary
    #Name_Dictionary = Exc_dict(Name_Dictionary, 'Franck Zambo','Andre-Frank Zambo Anguissa')
    #Name_Dictionary = Exc_dict(Name_Dictionary, 'Bobby Reid','Bobby Decordova-Reid')
    Name_Dictionary = Exc_dict(Name_Dictionary, 'Emerson','Emerson Aparecido Leite de Souza Junior')
    Name_Dictionary = Exc_dict(Name_Dictionary, 'Nicolas N&#039;Koulou','Nicolas Nkoulou')
    #display(Name_Dictionary)

    if not Table_FPL.empty:
        for i in range(len(Schedule)):
            if Schedule.at[i,'isResult']:
                #print(Schedule.at[i,'id'])
                MP, Name_Dictionary = add_match_to_dict(Schedule.at[i,'id'], Name_Dictionary)
                Table_Understat = Table_Understat.append(MP, ignore_index=True)

    print(f'\t All Data Downloaded.\t It takes {time() - start} sec')
    start = time()

    # Add fpl_id and name_fpl for players not in FPL
    j=0
    for i in Name_Dictionary.index:
        j+=1
        if Name_Dictionary.at[i,'name_fpl']=='':
            Name_Dictionary.at[i,'name_fpl'] = Name_Dictionary.at[i,'name_un']
            Name_Dictionary.at[i,'id_fpl'] = 1000000 + j

    name2id = dict(zip(Name_Dictionary['name_fpl'], Name_Dictionary['id_fpl']))
    Table_Understat['element'] = [name2id[Table_Understat.at[i, 'player']] for i in Table_Understat.index]
    Table_Understat = constti.change_column_name(Table_Understat, 'xG', 'threat')
    Table_Understat['threat']  = [100*float(Table_Understat['threat'][i]) for i in range(len(Table_Understat))]
    Table_Understat = constti.change_column_name(Table_Understat, 'xA', 'creativity')
    Table_Understat['creativity']  = [100*float(Table_Understat['creativity'][i]) for i in range(len(Table_Understat))]
    Table_Understat = constti.change_column_name(Table_Understat, 'player_id', 'Understat_id')
    Table_Understat = constti.change_column_name(Table_Understat, 'time', 'minutes')

    Table_Understat.to_csv(Path('in/Table_Understat.csv'), index=False)
    Name_Dictionary.to_csv(Path('in/Name_Dictionary.csv'), index=False)

    print(f'\t All Columns Added.\t It takes {time() - start} sec')
    print(f'inputUnderstat is over.\t It takes {time() - start_module} sec\n')

    if __name__ == '__main__':
        display(Table_Understat)