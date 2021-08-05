#!/usr/bin/env python
# coding: utf-8

# In[1]:


''' Functions especially for Brr project
'''

import pandas as pd
import numpy as np
import constti

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

def is_finished(n, Fixtures):
    '''
        Fills unfinished matches
    '''
    if n=='':
        return False
    elif np.isnan(n):
        return np.nan
    else:
        a = Fixtures[Fixtures['id']==n]['finished']
        return a.bool()

def del_empty_col(table):
    '''
        Deleting empty columns
    '''
    df = table.copy()
    df = df.applymap(lambda x: np.nan if x==[] else x)
    for i in df.columns:
        try:
            if [np.isnan(df[i].tolist()[j]) for j in range(len(df))]==[True for _ in df.index]:
                 del table[i]
        except:
            pass
    return table

def get_gw_num(col_name):
    '''
        returns num for standart column name 'GW11**' -> 11
        Used by no_lists, to_lists
    '''
    try:
        return int(col_name.replace('GW','').replace('*',''))
    except:
        return -1
    
def table_increasing(table):
    colN = [get_gw_num(table.columns[i]) for i in range(len(table.columns))]
    colNN = []
    for i in range(len(colN)):
        if colN[i]>-1:
            colNN.append(colN[i])
    colNNN = np.array(colNN[1:]) - np.array(colNN[:-1])
    for i in range(len(colNNN)):
        if colNNN[i]<0:
            return False
    return True
    
def no_lists(t):
    '''
        removes lists from table adding new column instead
    '''
    
    def GW_insert(table, col_name):
        '''
            insert columns after if column order is increasing or before if decreasing
        '''
            
        if (col_name[:2] == 'GW')&table_increasing(table):
            table.insert(list(table.columns).index(col_name)+1,col_name+'*',None)
            return table
        else:
            table.insert(list(table.columns).index(col_name),col_name+'*',None)
            return table
        
        
    table = t.copy()
    for i in t.index:
        for j in t.columns:
            if type(table.at[i,j]) in {list, np.ndarray}:
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

def to_lists(table):
    '''
        Adds lists (opposite to no_lists)
    '''
    if table_increasing(table):
        df  = table.copy()
        df = df.apply(lambda x:[[] if constti.myisnan(x[i]) else [x[i]] for i in range(len(x))] if x.name[:2]=='GW' else x)
        for i in range(len(df.columns)-1,0,-1):
            if df.columns[i].replace('*', '') == df.columns[i-1].replace('*', ''):
                for j in range(len(df)):
                    df.iat[j, i-1] = df.iat[j, i-1] + df.iat[j, i]
                del df[df.columns[i]]
    else:
        A  = to_lists(table[table.columns[::-1]])
        return A[A.columns[::-1]]
    return df

def no_last_GW(df):
    column_numbers = np.asarray(list(map(get_gw_num, df.columns)))
    return df[df.columns[column_numbers != max(column_numbers)]]


# In[ ]:




