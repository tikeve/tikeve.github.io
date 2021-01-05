import pandas as pd
import numpy as np

def toint(a):
    '''
        NaNs to zeros
    '''
    if np.isnan(a):
        return 0
    else: return int(a)


def noZ(a):
    '''
        If no matches played not to devide by zero
    '''
    b = a.copy()
    for i in range(len(b)):
        if b[i] == 0:
            b[i]=1
    return b

def is_finished(n):
    '''
        Fills unfinished matches
    '''
    if n=='':
        return False
    else:
        a = Fixtures[Fixtures['id']==n]['finished']
        return a.bool()

def del_empty_col(df):
    '''
        Deleting empty columns
    '''
    for i in df.columns:
        if (df[i].tolist()==[[] for _ in df.index])|\
        ([np.isnan(df[i].tolist()[j]) for j in range(len(df))]==[True for _ in df.index]):
             del df[i]
    return df


def no_lists(t, empty=np.nan):
    '''
        removes lists from table adding new column instead
    '''
    def get_gw_num(col_name):
        '''
            returns num for standart column name 'GW11**' -> 11
        '''
        try:
            return int(col_name.replace('GW','').replace('*',''))
        except:
            return -1
    
    def GW_insert(table, col_name):
        '''
            insert columns after if column order is increasing or before if decreasing
        '''
        #if col_name starts with 'GW' and next or previous column name with number larger or less then insert after
        if list(table.columns).index(col_name)<len(table.columns)-1:
            c1 = (get_gw_num(col_name) < get_gw_num(table.columns[list(table.columns).index(col_name)+1]))
            c2 = (get_gw_num(col_name) > 0)
            c3 = (get_gw_num(table.columns[list(table.columns).index(col_name)+1]) > 0)
            cond1 = c1&c2&c3
        else: cond1 = False
            
        if list(table.columns).index(col_name) > 0:
            c1 = (get_gw_num(col_name) > get_gw_num(table.columns[list(table.columns).index(col_name)-1]))
            c2 = (get_gw_num(col_name) > 0)
            c3 = (get_gw_num(table.columns[list(table.columns).index(col_name)-1]) > 0)
            cond2 = c1&c2&c3
        else: cond2 = False
            
        if (col_name[:2] == 'GW')&(cond1|cond2):
            table.insert(list(table.columns).index(col_name)+1,col_name+'*',None)
            return table
        else:
            table.insert(list(table.columns).index(col_name),col_name+'*',None)
            return table
        
        
    table = t.copy()
    for i in table.index:
        for j in table.columns:
            if type(table.at[i,j])==list:
                if len(table.at[i,j])==0:
                    table.at[i,j]=None  #''
                elif len(table.at[i,j])==1:
                    table.at[i,j]=table.at[i,j][0]
                elif  len(table.at[i,j])==2:
                    if j+'*' not in table.columns:
                        #table.insert(list(table.columns).index(j),j+'*',None)
                        table = GW_insert(table, j)
                        table[j+'*'] = [None for _ in range(len(table))]
                    table.at[i,j+'*'] = table.at[i,j][1]
                    table.at[i,j]=table.at[i,j][0]
                else:#len(table.at[i,j])==3
                    if j+'*' not in table.columns:
                        table = GW_insert(table, j)
                    if j+'**' not in table.columns:
                        table = GW_insert(table, j+'*')
#                     if j+'*' not in table.columns:
#                         table.insert(list(table.columns).index(j),j+'*',None)
#                     if j+'**' not in table.columns:
#                         table.insert(list(table.columns).index(j)-1,j+'**',None)
                    table.at[i,j+'**'] = table.at[i,j][2]
                    table.at[i,j+'*'] = table.at[i,j][1]
                    table.at[i,j]=table.at[i,j][0]
    return table.fillna(np.nan).applymap(lambda x: x if type(x) != str else np.nan if x=='' else x) 