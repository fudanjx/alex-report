"""
putting some comment
"""
import os
import pandas as pd
import numpy as np
import data_prep as prep              # custom module for file cleaning
import path as PT

df = pd.DataFrame()
file = "Admission2020.XLS"
path = 'D:/Dropbox/HIM/Raw_Download/Admission/'
# Raw data file should be saved at others

# inspect the first 50 rows to find out which row start with actual data header.
# Assume all report will have case number field
# idx is the index row for the actual data starts, use this value to skip header when do 2nd import
df = pd.read_table(path + file, sep='/t',
                   skip_blank_lines=False, nrows=50,
                   engine='python')
col = df.columns
idx = df[df[col[0]].astype(str).str.contains("Case")].index.values

# read raw download file with 'skiprows' based on idx value obtained

df = pd.read_csv(path + file, sep='\t',
                 skiprows=idx[0]+1, skipfooter=1, skip_blank_lines=True,
                 engine='python')

print(df)
print(df.shape)
# find all the "unnamed" column which are empty column, and remove them
cols = [item for item in df.columns if item.lower()[:7] != 'unnamed']
df = df[cols]
# clean the header, in case there are trailing white space
headers = [header for header in df.columns]  # find header, using list comprehensions
headers = [header.replace('.', ' ') for header in headers]  # replace any header with . format
headers = [header.strip() for header in headers]  # Trim leading and trailing space
headers = [header.replace(' ', '_') for header in headers]
headers = [header.replace('__', '_') for header in headers]
headers = [header.replace('/', '_') for header in headers]
df.columns = headers  # replace header
# remove empty rows and duplicated header due to original paged data.
# Assume all reports will have 'Case No' field, use it to filter for AlEX cases (2800) NUH case (15xx)

print(df)
print(df.shape)

df = df.loc[df['Case_No'].notna(), :]
df = df.loc[df['Case_No'].str.contains('2800'), :]   # NUH case number start with 15
df['cnt'] = 1
df = prep.Date_Conversion(df)

print(df)
print(df.shape)

print(file, " Processed")

# to avoid the encoding error
# df['Postal_Code'] = df['Postal_Code'].str.slice(0, 6)

df.to_csv(PT.path_wip_output + 'single_SOC.csv', index=False)
df.to_parquet(PT.path_wip_output+"single_SOC.parquet", index=False)
print("Other data: ", df.shape)
print("Extraction successfully completed - data clean up done")
