import datetime
import os
import time
import path as PT
import pandas as pd
import numpy as np
from datetime import datetime

# set data frame display
desired_width = 6200
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 4)


# read file information from any directory include sharepoint. and return dataframe of the files info
def get_information(directory):
    df = pd.DataFrame(columns=['SPOC_Name', 'most_recent_modify', 'created', 'Status', 'Cat', 'report time', 'url'])
    dir = directory
    for i in os.listdir(directory):
        file_list = []
        a = os.stat(os.path.join(directory, i))
        p = 'https:' + directory + '/' + i  # obtain the exact url link
        file_list = [i[:-5], time.ctime(a.st_mtime), time.ctime(a.st_ctime)]  # [file,most_recent_access,created]
        if file_list[1] == file_list[2]:
            status = "Pending Review"
        else:
            status = "Reviewed"
        file_list.append(status)
        file_list.append(dir[28:])
        file_list.append(datetime.now())
        file_list.append(p)
        if file_list[0] != 'M':
            to_append = file_list
            df_length = len(df)
            df.loc[df_length] = to_append
    return df


def get_col_widths(dataframe):
    # First we find the maximum length of the index column
    idx_max = max([len(str(s)) for s in dataframe.index.values] + [len(str(dataframe.index.name))])
    # Then, we concatenate this to the max of the lengths of column name and its values for each column, left to right
    return [idx_max] + [max([len(str(s)) for s in dataframe[col].values] + [len(col)]) for col in dataframe.columns]


def recode_status(aaa):
    if aaa == 'Pending Review':
        return 1
    else:
        return 0


def embed_hyperlink(df):
    for index_df in df.index:
        url_CS = df.loc[index_df, 'UAM (Clinical Systems)_y']
        mainFile = df.loc[index_df, 'UAM (Clinical Systems)_x']
        df.loc[index_df, 'UAM (Clinical Systems)'] = '=HYPERLINK("{}","{}")'.format(url_CS, mainFile)

        url_CS = df.loc[index_df, 'UAM (DDS)_y']
        mainFile = df.loc[index_df, 'UAM (DDS)_x']
        df.loc[index_df, 'UAM (DDS)'] = '=HYPERLINK("{}","{}")'.format(url_CS, mainFile)

        url_CS = df.loc[index_df, 'UAM (SAP PAPM)_y']
        mainFile = df.loc[index_df, 'UAM (SAP PAPM)_x']
        df.loc[index_df, 'UAM (SAP PAPM)'] = '=HYPERLINK("{}","{}")'.format(url_CS, mainFile)

        url_CS = df.loc[index_df, 'UAM (SAP FIMM)_y']
        mainFile = df.loc[index_df, 'UAM (SAP FIMM)_x']
        df.loc[index_df, 'UAM (SAP FIMM)'] = '=HYPERLINK("{}","{}")'.format(url_CS, mainFile)

    df = df[['UAM (Clinical Systems)', 'UAM (DDS)', 'UAM (SAP FIMM)', 'UAM (SAP PAPM)']]
    df = df.replace('=HYPERLINK("nan","nan")', "-")
    return df


search_dir_SAPPAPM = r'//nuhs-doc/AH/Operations/DG/UAM (SAP PAPM)'
search_dir_SAPFIMM = r'//nuhs-doc/AH/Operations/DG/UAM (SAP FIMM)'
search_dir_DDS = r'//nuhs-doc/AH/Operations/DG/UAM (DDS)'
search_dir_Clinical = r'//nuhs-doc/AH/Operations/DG/UAM (Clinical Systems)'

df_SAPPAPM = get_information(search_dir_SAPPAPM)
df_SAPFIMM = get_information(search_dir_SAPFIMM)
df_DDS = get_information(search_dir_DDS)
df_Clinical = get_information(search_dir_Clinical)

# to concat and generate the summary report
df_all = pd.concat([df_SAPPAPM, df_SAPFIMM, df_DDS, df_Clinical])
df_all['cnt'] = 1

df_pivot = pd.pivot_table(df_all, values='cnt', index=['Cat'],
                          columns=['Status'],
                          aggfunc=np.sum, margins=True, margins_name='Total')
# add additional column to show % Pending Review
df_pivot['%Completion'] = 1 - df_pivot['Pending Review'] / df_pivot['Total']
df_pivot['%Completion'] = df_pivot['%Completion'].astype(float).map("{:.1%}".format)
df_pivot['Status_date'] = datetime.today().strftime("%d/%m/%Y")
today_str = datetime.today().strftime("%d.%m.%Y")

print(df_pivot)

df_all['cnt_status'] = df_all.apply(lambda x: recode_status(x['Status']), axis=1)
df_pivot_detail = pd.pivot_table(df_all, values='cnt_status', index=['SPOC_Name'],
                                 columns=['Cat'], aggfunc=np.sum, margins=True, margins_name='Total')
df_pivot_detail.sort_values(by=['Total'], ascending=False, inplace=True)
df_pivot_detail.drop(columns=["Total"], index=["Total"], inplace=True)
df_pivot_detail.replace(1, 'Pending Review', inplace=True)
df_pivot_detail.replace(0, 'Reviewed', inplace=True)

# create df for all the hyper link
df_hyper = df_all.pivot(index='SPOC_Name', columns='Cat', values='url')

# merge the review status and hyperlink into same dataframe
df_combined = pd.merge(df_pivot_detail, df_hyper, how='left', on='SPOC_Name')
print(df_combined)
# using embed function to inject the url into each cell, and clean up for the final dataframe
df_final = embed_hyperlink(df_combined)

# reset index and start with 1
df_SAPPAPM = df_SAPPAPM.sort_values(by=['Status']).reset_index(drop=True)
df_SAPPAPM.index = df_SAPPAPM.index + 1

df_SAPFIMM = df_SAPFIMM.sort_values(by=['Status']).reset_index(drop=True)
df_SAPFIMM.index = df_SAPFIMM.index + 1

df_DDS = df_DDS.sort_values(by=['Status']).reset_index(drop=True)
df_DDS.index = df_DDS.index + 1

df_Clinical = df_Clinical.sort_values(by=['Status']).reset_index(drop=True)
df_Clinical.index = df_Clinical.index + 1

# write to excel report
writer = pd.ExcelWriter(PT.path_report_output + 'UAM_Status(' + today_str + ').xlsx', engine='xlsxwriter')
df_pivot.to_excel(writer, sheet_name='Summary')
df_final.to_excel(writer, sheet_name='Summary_Details')
df_SAPPAPM.to_excel(writer, sheet_name='PAPM')
df_SAPFIMM.to_excel(writer, sheet_name='FIMM')
df_DDS.to_excel(writer, sheet_name='DDS')
df_Clinical.to_excel(writer, sheet_name='Clinical')

# prepare formatting with xlsxwriter
workbook = writer.book
worksheet0 = writer.sheets['Summary']
worksheet00 = writer.sheets['Summary_Details']
worksheet1 = writer.sheets['PAPM']
worksheet2 = writer.sheets['FIMM']
worksheet3 = writer.sheets['DDS']
worksheet4 = writer.sheets['Clinical']

# set column width to fit the longest string length
for i, width in enumerate(get_col_widths(df_pivot)):
    worksheet0.set_column(i, i, width + 6)
for i, width in enumerate(get_col_widths(df_pivot_detail)):
    worksheet00.set_column(i, i, width + 4)
for i, width in enumerate(get_col_widths(df_SAPPAPM)):
    worksheet1.set_column(i, i, width + 4)
for i, width in enumerate(get_col_widths(df_SAPFIMM)):
    worksheet2.set_column(i, i, width + 4)
for i, width in enumerate(get_col_widths(df_DDS)):
    worksheet3.set_column(i, i, width + 4)
for i, width in enumerate(get_col_widths(df_Clinical)):
    worksheet4.set_column(i, i, width + 4)

# format to highlight those "Pending review"
format_01 = workbook.add_format({'bg_color': '#FFC7CE',
                                 'font_color': '#9C0006'})
worksheet00.conditional_format('$B2:$F200', {'type': 'text',
                                             'criteria': 'containing',
                                             'value': 'Pending',
                                             'format': format_01})
worksheet1.conditional_format('$E2:$E200', {'type': 'text',
                                            'criteria': 'containing',
                                            'value': 'Pending',
                                            'format': format_01})
worksheet2.conditional_format('$E2:$E200', {'type': 'text',
                                            'criteria': 'containing',
                                            'value': 'Pending',
                                            'format': format_01})
worksheet3.conditional_format('$E2:$E200', {'type': 'text',
                                            'criteria': 'containing',
                                            'value': 'Pending',
                                            'format': format_01})
worksheet4.conditional_format('$E2:$E200', {'type': 'text',
                                            'criteria': 'containing',
                                            'value': 'Pending',
                                            'format': format_01})

writer.save()

print("UAM Reports exported and saved ")
