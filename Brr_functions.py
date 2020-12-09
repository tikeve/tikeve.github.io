#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
#NaNs to zeros
def toint(a):
    if np.isnan(a):
        return 0
    else: return int(a)

#If no matches played not to devide by zero
def noZ(a):
    b = a.copy()
    for i in range(len(b)):
        if b[i] == 0:
            b[i]=1
    return b

#Kills unfinished matches
def is_finished(n):
    if n=='':
        return False
    else:
        a = Fixtures[Fixtures['id']==n]['finished']
        return a.bool()

#Deleting empty columns
def del_empty_col(df):
    for i in df.columns:
        if (df[i].tolist()==[[] for _ in df.index])|        ([np.isnan(df[i].tolist()[j]) for j in range(len(df))]==[True for _ in df.index]):
             del df[i]
    return df
#removes lists from table adding new column instead
def no_lists(t, empty=np.nan):
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
                        table.insert(list(table.columns).index(j),j+'*',None)
                        table[j+'*'] = [None for _ in range(len(table))]
                    table.at[i,j+'*'] = table.at[i,j][1]
                    table.at[i,j]=table.at[i,j][0]
                else:#len(table.at[i,j])==3
                    if j+'*' not in table.columns:
                        table.insert(list(table.columns).index(j),j+'*',None)
                    if j+'**' not in table.columns:
                        table.insert(list(table.columns).index(j)+1,j+'**',None)
                    table.at[i,j+'**'] = table.at[i,j][2]
                    table.at[i,j+'*'] = table.at[i,j][1]
                    table.at[i,j]=table.at[i,j][0]
    return table.applymap(lambda x: np.nan if x == '' else    x if type(x) in (int, np.int64,  float, np.float64, list, np.ndarray, str) else np.nan)


# In[ ]:




