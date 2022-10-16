#Script for getting  Jira Stories stats


import os
import warnings
from datetime import timedelta

import numpy as np
import pandas as pd
from jira import JIRA
from pyspark.sql import SparkSession

warnings.filterwarnings('ignore')


login = os.getenv('JIRA_LOGIN')
password = os.getenv('JIRA_PASSWORD')




jira_options = {'server': 'https://myjira.com'}
jira = JIRA(options=jira_options, basic_auth=(login, password))


rows = []

issues = jira.search_issues('project=  55555 and issuetype = Story and created >=2022-01-01', maxResults=1000, expand='changelog')

for issue in issues:
    issue.key,
    issue.fields.summary,
    issue.fields.issuetype.name,
    issue.fields.created

    for history in issue.changelog.histories:
        history.created

        for item in history.items:
            item.field,
            item.fromString,
            item.toString


            rows.append((issue.key,
                         issue.fields.summary,
                         issue.fields.issuetype.name,
                         issue.fields.created,
                         history.created,
                         item.field,
                         item.fromString,
                         item.toString))


#Create an array and then dataframe
result = np.array(rows)
df = pd.DataFrame(np.array(result))


df = df.rename(columns={ 0: 'Key',
                         1: 'Summary',
                         2: 'Type',
                         3: 'Created',
                         4: 'Change time',
                         5: 'Change field',
                         6: 'fromString',
                         7: 'toString'
                         })

#Leave lines with status
df2 = df.loc[(df['Change field'] == 'status')]


# Leave the desired statuses
df3 = df2[(df2['fromString'] == 'To Do')
          | (df2['toString'] == 'Analysis')
          | (df2['toString'] == 'Done')]

# Formate the time from 2022-03-23T16:53:40.000+0300 to '%Y-%m-%d %H:%M'
df3['Change time'] = pd.to_datetime(df3['Change time']) - timedelta(hours=3)
df3['Change time']  = df3['Change time'].dt.strftime('%Y-%m-%d %H:%M')
df3['Created'] = pd.to_datetime(df3['Created']) - timedelta(hours=3)
df3['Created']  = df3['Created'].dt.strftime('%Y-%m-%d %H:%M')

# Create separate columns for each status
df3['To_Do'] = df3['Change time'].where(df3['fromString']=='To Do')
df3['Analysis'] = df3['Change time'].where(df3['toString']=='Analysis')
df3['Done'] = df3['Change time'].where(df3['toString']=='Done')


#Create composite keys later to drop duplicates
df3["Key+From"] = df3["Key"]+df3["fromString"]
df3["Key+To"] = df3["Key"]+df3["toString"]

#Create separate df for three statuses
df3_todo = df3.loc[(df3['fromString'] == 'To Do')]
df3_an = df3.loc[(df3['toString'] == 'Analysis')]
df3_done = df3.loc[(df3['toString'] == 'Done')]

# irrelevant: for To Do and Analysis we leave the first match in the status change log (first), for Done - last (last)
# relevant: for all three statuses, we leave the last match:

df3_todo.drop_duplicates(subset = ['Key+From'], keep = 'last', inplace = True)
df3_an.drop_duplicates(subset = ['Key+To'], keep = 'last', inplace = True)
df3_done.drop_duplicates(subset = ['Key+To'], keep = 'last', inplace = True)

#Leave the desired columns
df3_todo = df3_todo[["Key", "Summary","Created", 'To_Do']]
df3_an = df3_an[["Key",  "Analysis"]]
df3_done = df3_done[["Key",  "Done"]]

# Left join for three dfs
df_fin = pd.concat(
    (KeyF.set_index('Key') for KeyF in [df3_todo, df3_an,  df3_done]),
    axis=1, join='outer'
).reset_index()

#Add columns counting days/hours between 'To Do' and 'Done'
df_fin[['To_Do','Done']] = df_fin[['To_Do','Done']].apply(pd.to_datetime)
df_fin['Count_Hours'] = ((df_fin['Done'] - df_fin['To_Do']).dt.total_seconds() / 60 / 60).round(decimals = 2)
df_fin['Count_Days'] = (df_fin['Count_Hours'] / 24).round(decimals = 2)
df_fin['To_Do'] = df_fin['To_Do'].dt.strftime('%Y-%m-%d %H:%M')
df_fin['Done'] = df_fin['Done'].dt.strftime('%Y-%m-%d %H:%M')


[...]

#Replace values with NaN

df_fin_pandas = df_fin.fillna(0)

#df_fin_pandas = df_fin2[['index', 'Summary','Created', 'To_Do','Analysis','Done','Count_Hours','Count_Days']]


#Replace negative values
df_fin_pandas['Count_Hours'] = df_fin_pandas['Count_Hours'].apply(lambda x : x if x > 0 else 0.00)
df_fin_pandas['Count_Days'] = df_fin_pandas['Count_Days'].apply(lambda x : x if x > 0 else 0.00)
print(df_fin_pandas.head(9))

df_fin_pandas2 = df_fin_pandas[['index', 'Summary','Created', 'To_Do','Analysis','Done','Count_Hours','Count_Days']]

df_fin_pandas2[['index', 'Summary','Created', 'To_Do','Analysis','Done','Count_Hours','Count_Days']] = df_fin_pandas2[['index', 'Summary','Created', 'To_Do','Analysis','Done','Count_Hours','Count_Days']].astype(str)



