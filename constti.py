''' My Useful Functions
'''

import unicodedata
import numpy as np
import requests
import glob, os
from bs4 import BeautifulSoup

# 1. Tries to download page 6 times instead of 1
def long_request(url):
    for j in range(6):
        try:
            p = requests.get(url)
            return p
            #break
        except Exception as e:
            print(e)
    return 'CAN_NOT_CONNECT'

# 2. Checks if list has the same elements
def Doubles(a):
    ans = []
    for i in range(1,len(a)):
        if a[i] in set(a[:i]):
            ans.append(a[i])
    return ans

# 3. Checks if a table has columns or rows with the same name.(That's a huge mistake!)
def DRDC(table):
    a = ['raws']
    a.extend(Doubles(table.index))
    a.append('columns')
    a.extend(Doubles(table.columns))
    return a

# 4. Delete accents above players names
def strip_accents(s):
   return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')

# 5. strip_accents for the 1st clumn of the table
# Need to be DataFrame,not a column constti.strip_accents_pdlist(pd.DataFrame(bigTable['web_name']))
def strip_accents_pdlist(a):
    b = a.copy()
    for i in range(len(a)):
        b.iat[i,0] = strip_accents(a.iat[i,0])
    return b

# 6. Changes name of a column col_name to new_col_name of a Table
def change_column_name(Table, col_name, new_col_name):
    if new_col_name in set(Table.columns):
        print("Can't change column name")
        return Table
    temp_Table = Table.copy()
    temp = temp_Table.columns
    temp = [temp[i] if temp[i] != col_name else new_col_name for i in range(len(temp))]
    temp_Table.columns = temp
    return temp_Table

# 7. Finds differences between the tables

def differences(A, B):
    '''
        Finds differences between the tables
        Change None to np.nan previously
    '''
    same = 1
    if list(A.index) != list(B.index):
        print(f'Indexes are not equal: {(set(A.index)-set(B.index))|(set(B.index)-set(A.index))}')
        same = 0
    if list(A.columns) != list(B.columns):
        print(f'Columns are not equal: {(set(A.columns)-set(B.columns))|(set(B.columns)-set(A.columns))}')
        same = 0
    if same == 0:
        return A
    else:
        A = A.fillna(value=np.nan)
        B = B.fillna(value=np.nan)
        def eqNeq(col):
            res = col.copy()
            for i in A.index:
                a = type(col[i]) not in (float, np.float64)
                b = type(B[col.name][i]) not in (float, np.float64)
                if a|b: res[i] = bool(col[i] == B[col.name][i])
                else:
                    c = np.isnan(col[i])
                    d = np.isnan(B[col.name][i])
                    if c&d: res[i] = True
                    else: res[i] = bool(col[i] == B[col.name][i])
            return res
        df = A.apply(eqNeq)
#         df = A.apply(lambda x: [x[i] == B[x.name][i] \
#         if (type(x[i]) not in (float, np.float64))|(type(B[x.name][i]) not in (float, np.float64))\
#         else True if (np.isnan(x[i]))&(np.isnan(B[x.name][i]))\
#         else x[i] == B[x.name][i] for i in A.index])

        for i in df.index:
            if df.loc[i].sum() == len(df.columns):
                df = df.drop(i)
        for j in df.columns:
            if df[j].sum() == len(df):
                del df[j]
        return df
    
# 8. isnan without mistake for non numpy objects

def myisnan(x):
    try:
        return np.isnan(x)
    except:
        return False

# 9. Same head, header and script at the end for all html files inside html folder

def head_header_script (copy_head = 1, copy_header = 1, copy_script = 1, copy_nav=1):
    '''
        Same head, header and script at the end for all html files inside html folder
    '''
    with open('index.html', 'r', encoding="utf-8") as file:
        main_html = file.read()
        header = str(BeautifulSoup(main_html, 'html.parser')('header')[0])
        head = str(BeautifulSoup(main_html, 'html.parser')('head')[0])
        script = str(BeautifulSoup(main_html, 'html.parser')('script')[-1])
        nav = str(BeautifulSoup(main_html, 'html.parser')('nav')[0])

    my_path = os.getcwd()
    file_pathes = glob.glob(my_path + '/html/**/*.html', recursive=True)

    for path in file_pathes:
        with open(path, 'r', encoding="utf-8") as file:
            print(path)
            file_text = file.read()
            
        if (file_text.find('<head>') != -1) & (copy_head == 1):
            new_file = file_text.replace(file_text[file_text.find('<head>'):file_text.find('</head>')+7], head)
        if (file_text.find('<header>') != -1) & (copy_header == 1):
            new_file = new_file.replace(new_file[new_file.find('<header>'):new_file.find('</header>')+9], header)
        if (new_file.find('<script id="end">') != -1) & (copy_script == 1):
            #print("found!!")
            new_file = new_file.replace(new_file[new_file.find('<script id="end">'):], script)
        else:
            #print("not found")
            new_file = new_file + "\n" + script
        if (file_text.find('<nav>') != -1) & (copy_nav == 1):
            new_file = new_file.replace(new_file[new_file.find('<nav>'):new_file.find('</nav>')+6], nav)
    #     if new_file.find('/n<script id="end">') != -1:
    #         new_file = new_file.replace('/n<script id="end">', '<script id="end">')
        with open(path, 'w', encoding="utf-8") as file:
            file.write(new_file)

if __name__=="__main__":
    print('Hello')
    p = long_request('http://google.com')
    print(p.text)