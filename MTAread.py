import pandas as pd
import numpy as np

set171 = ['170304','170311','170318','170325']
set172 = ['170401','170408','170415','170422']
set173 = ['170429','170506','170513','170520']
set174 = ['170527','170603']
set161 = ['160305','160312','160319','160326']
set162 = ['160402','160409','160416','160423']
set163 = ['160430','160507','160514','160521']
set164 = ['160528','160604']
list171=[pd.DataFrame()]*4;
list161=[pd.DataFrame()]*4;
list172=[pd.DataFrame()]*4;
list162=[pd.DataFrame()]*4;
list173=[pd.DataFrame()]*4;
list163=[pd.DataFrame()]*4;
list174=[pd.DataFrame()]*2;
list164=[pd.DataFrame()]*2;
for i in range(4):
    str17 = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_" + set171[i] + ".txt"
    str16 = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_" + set161[i] + ".txt"
    list171[i] = pd.read_csv(str17)
    list161[i] = pd.read_csv(str16)
y171 = pd.concat(list171)
y161 = pd.concat(list161)

for i in range(4):
    str17 = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_" + set172[i] + ".txt"
    str16 = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_" + set162[i] + ".txt"
    list172[i] = pd.read_csv(str17)
    list162[i] = pd.read_csv(str16)
y172 = pd.concat(list172)
y162 = pd.concat(list162)

for i in range(4):
    str17 = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_" + set173[i] + ".txt"
    str16 = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_" + set163[i] + ".txt"
    list173[i] = pd.read_csv(str17)
    list163[i] = pd.read_csv(str16)
y173 = pd.concat(list173)
y163 = pd.concat(list163)

for i in range(2):
    str17 = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_" + set174[i] + ".txt"
    str16 = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_" + set164[i] + ".txt"
    list174[i] = pd.read_csv(str17)
    list164[i] = pd.read_csv(str16)
y174 = pd.concat(list174)
y164 = pd.concat(list164)

y17 = pd.concat([y171,y172,y173,y174])
y16 = pd.concat([y161,y162,y163,y164])
y17.columns = [column.strip() for column in y17.columns]
y16.columns = [column.strip() for column in y16.columns]

y17['DATETIME'] = [pd.to_datetime(i+j, format = '%m/%d/%Y%H:%M:%S') for i, j in zip(y17.DATE, y17.TIME)]
y16['DATETIME'] = [pd.to_datetime(i+j, format = '%m/%d/%Y%H:%M:%S') for i, j in zip(y16.DATE, y16.TIME)]
y17.sort_values(['DATETIME']).groupby(['C/A','UNIT','SCP','STATION','LINENAME','DIVISION'])
y17['ENTRIESDOWN'] = y17.groupby(['C/A','UNIT','SCP','STATION','LINENAME','DIVISION']).ENTRIES.shift()
y17['EXITSDOWN'] = y17.groupby(['C/A','UNIT','SCP','STATION','LINENAME','DIVISION']).EXITS.shift()
y16.sort_values(['DATETIME']).groupby(['C/A','UNIT','SCP','STATION','LINENAME','DIVISION'])
y16['ENTRIESDOWN'] = y16.groupby(['C/A','UNIT','SCP','STATION','LINENAME','DIVISION']).ENTRIES.shift()
y16['EXITSDOWN'] = y16.groupby(['C/A','UNIT','SCP','STATION','LINENAME','DIVISION']).EXITS.shift()
y17.dropna(inplace=True)
y16.dropna(inplace=True)
y17['ENTRIES'] = [i - int(j) for i,j in zip(y17['ENTRIES'], y17['ENTRIESDOWN'])]
y17['EXITS'] = [i - int(j) for i,j in zip(y17['EXITS'], y17['EXITSDOWN'])]
y16['ENTRIES'] = [i - int(j) for i,j in zip(y16['ENTRIES'], y16['ENTRIESDOWN'])]
y16['EXITS'] = [i - int(j) for i,j in zip(y16['EXITS'], y16['EXITSDOWN'])]
del y16['ENTRIESDOWN']
del y16['EXITSDOWN']
del y17['ENTRIESDOWN']
del y17['EXITSDOWN']

pd.y17.to_csv('year2017.csv')
pd.y16.to_csv('year2016.csv')