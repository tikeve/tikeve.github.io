#!/usr/bin/env python
# coding: utf-8

# In[18]:


import pandas as pd
from pathlib import Path
from Source import write_table
import sys

try:
    Table = pd.read_csv(Path('in/Table_FPL.csv'))
except:
    sys.argv = ['', 'pre-season']
    folder = ''
    Teams = pd.read_csv(Path(f'{folder}in/Teams.csv'))
    Players = pd.read_csv(Path(f'{folder}in/Players.csv'))

    for source in ['FPL', 'Understat']:
#         TxG.to_csv(Path(f'{folder}mid/{source}/TxG.csv'), index=False)
#         TxA.to_csv(Path(f'{folder}mid/{source}/TxA.csv'), index=False)
#         TOxG.to_csv(Path(f'{folder}mid/{source}/TOxG.csv'), index=False)
#         TxGA.to_csv(Path(f'{folder}mid/{source}/TxGA.csv'), index=False)
#         TxAA.to_csv(Path(f'{folder}mid/{source}/TxAA.csv'), index=False)
#         TOxGA.to_csv(Path(f'{folder}mid/{source}/TOxGA.csv'), index=False)
#         PxG.to_csv(Path(f'{folder}mid/{source}/PxG.csv'), index=False)
#         PxA.to_csv(Path(f'{folder}mid/{source}/PxA.csv'), index=False)
#         PxGA.to_csv(Path(f'{folder}mid/{source}/PxGA.csv'), index=False)
#         PxAA.to_csv(Path(f'{folder}mid/{source}/PxAA.csv'), index=False)

#         TxxG.to_csv(Path(f'{folder}mid/{source}/TxxG.csv'), index=False)
#         TxxA.to_csv(Path(f'{folder}mid/{source}/TxxA.csv'), index=False)
#         TxOxG.to_csv(Path(f'{folder}mid/{source}/TxOxG.csv'), index=False)
#         PxxG.to_csv(Path(f'{folder}mid/{source}/PxxG.csv'), index=False)
#         PxxA.to_csv(Path(f'{folder}mid/{source}/PxxA.csv'), index=False)
#         PxOxG.to_csv(Path(f'{folder}mid/{source}/PxOxG.csv'), index=False)


        TxG = pd.DataFrame()
#         class_args = [Teams, Players, TAwu, TDwu, PAwu]
#         class_args += [self.source, self.ma_num, folder]
        class_args = [Teams, Players, 0, 0, 0, source, 0, '']
#         A = write_table(TxG, 'Team_xG', 'xG av', TxG, 100, 0, class_args)
        
#         del Teams['id']
#         del Players['web_name']
#         del Players['Team number']
        
        Teams.columns = ['id','Teams', 'xG av', 'Matches']
        A = write_table(Teams, 'Team_xG', 'xG av', pd.DataFrame(), 100, 0, class_args)
        Teams.to_csv(Path(f'{folder}out/{source}/Team_xG.csv'), index=False)
        
        Teams.columns = ['id','Teams', 'xA av', 'Matches']
        A = write_table(Teams, 'Team_xA', 'xA av', pd.DataFrame(), 100, 0, class_args)
        Teams.to_csv(Path(f'{folder}out/{source}/Team_xA.csv'), index=False)
        
        Teams.columns = ['id','Teams', 'Opponent xG av', 'Matches']
        A = write_table(Teams, 'Team_Opponent_xG', 'Opponent xG av', pd.DataFrame(), 100, 0, class_args)
        Teams.to_csv(Path(f'{folder}out/{source}/Team_Opponent_xG.csv'), index=False)
        
        Teams.columns = ['id','Teams', 'xG av adj', 'Matches']
        A = write_table(Teams, 'Team_xG_Ad', 'xG av adj', pd.DataFrame(), 100, 0, class_args)
        Teams.to_csv(Path(f'{folder}out/{source}/Tema_xG_Ad.csv'), index=False)
        
        Teams.columns = ['id','Teams', 'xA av adj', 'Matches']
        A = write_table(Teams, 'Team_xA_Ad', 'xA av adj', pd.DataFrame(), 100, 0, class_args)
        Teams.to_csv(Path(f'{folder}out/{source}/Team_xA_Ad.csv'), index=False)
        
        Teams.columns = ['id','Teams', 'Opponent xG av adj', 'Matches']
        A = write_table(Teams, 'Team_Opponent_xG_Ad', 'Opponent xG av adj', pd.DataFrame(), 100, 0, class_args)
        Teams.to_csv(Path(f'{folder}out/{source}/Team_Opponent_xG_Ad.csv'), index=False)
        
        Teams.columns = ['id','Teams', 'xG adjusted', 'xG']
        Teams['xA adjusted'] = [0 for i in Teams.index]
        Teams['xA'] = [0 for i in Teams.index]
        Teams['Opponent xG adjusted'] = [0 for i in Teams.index]
        Teams['Opponent xG'] = [0 for i in Teams.index]
        A = write_table(Teams, 'TableTeams', 'xG adjusted', pd.DataFrame(), 100, 0, class_args)
        Teams.to_csv(Path(f'{folder}out/{source}/TableTeams.csv'), index=False)
        del Teams['xA adjusted']
        del Teams['xA']
        del Teams['Opponent xG adjusted']
        del Teams['Opponent xG']
        
        #Players.columns = ['Name', 'Team', 'Position']
        key_col = 'xG' + ' per fixture'
        key_col_game = key_col.replace('fixture','game')
        X = Players.copy()
        X['Team games'] = [0 for i in Players.index]
        X['Played'] = [0 for i in Players.index]
        n = X.columns.get_loc('Team games')
        X.insert( n, key_col, 0)
        X.insert(n + 1, key_col_game, 0)
        
        X.columns = ['id', 'Name', 'web_name', 'Team number', 'Team', 'Position', key_col, key_col_game,
                     'Team games', 'Played']
        A = write_table(X, 'Player_xG', key_col, pd.DataFrame(), 100, 0, class_args)
        Players.to_csv(Path(f'{folder}out/{source}/Player_xG.csv'), index=False)
        
        key_col = 'xA' + ' per fixture'
        key_col_game = key_col.replace('fixture','game')
        X.columns = ['id', 'Name', 'web_name', 'Team number', 'Team', 'Position', key_col, key_col_game,
                     'Team games', 'Played']
        A = write_table(X, 'Player_xA', key_col, pd.DataFrame(), 100, 0, class_args)
        Players.to_csv(Path(f'{folder}out/{source}/Player_xA.csv'), index=False)
        
        key_col = 'xG' + ' per fixture adj'
        key_col_game = key_col.replace('fixture','game')
        X.columns = ['id', 'Name', 'web_name', 'Team number', 'Team', 'Position', key_col, key_col_game,
                     'Team games', 'Played']
        A = write_table(X, 'Player_xG_Ad', key_col, pd.DataFrame(), 100, 0, class_args)
        Players.to_csv(Path(f'{folder}out/{source}/Player_xG_Ad.csv'), index=False)
        
        key_col = 'xA' + ' per fixture adj'
        key_col_game = key_col.replace('fixture','game')
        X.columns = ['id', 'Name', 'web_name', 'Team number', 'Team', 'Position', key_col, key_col_game,
                     'Team games', 'Played']
        A = write_table(X, 'Player_xA_Ad', key_col, pd.DataFrame(), 100, 0, class_args)
        Players.to_csv(Path(f'{folder}out/{source}/Player_xA_Ad.csv'), index=False)
        
        

    
    print("Table_FPL is empty or can't be read")

