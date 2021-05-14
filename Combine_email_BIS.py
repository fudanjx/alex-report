import pandas as pd
import numpy as np
import path as PT
import win32com
from win32com.client import Dispatch
import os

folder_path = PT.path_Email_data

# remove all the while space in the source email file names
folder_path = PT.path_Email_data
[os.rename(os.path.join(folder_path, f), os.path.join(folder_path, f).replace(' ', '_').lower()) for f in
 os.listdir(folder_path)]

# define the keyword list help to locate the key words within the email body
keywords = ["Ward 2", 'C - Emerald Unit', 'Ward 3', 'Ward 4', 'Ward 5', 'Ward 7', 'B1', 'Ward 12', 'Ward 13', 'Ward 8',
            'COVID-19 ISO', 'Ward 9', 'Ward 8', 'ICU1']
cls_list = ['A1', 'B1', 'B2', 'C', 'ISO', 'ICU']
ward_key_word = ['Ward', 'ICU1', 'ICU2']
temp_ward = 'Ward2'


# this function is to assign ward name based on the previous
# ward name if no ward name keyword presented in each string segment
def ward_name(list_to_check, ward_keyword_list):
    for item in list_to_check:
        if item in ward_keyword_list:
            idx = list_to_check.index(item)
            return item + " " + list_to_check[idx + 1]
        else:
            pass
    return False


# Connect to Outlook with MAPI
outlook = win32com.client.Dispatch('Outlook.Application').GetNamespace('MAPI')

# Initialise & populate list of emails
email_list = [file for file in os.listdir(folder_path) if file.endswith(".msg")]

# define a DataFrame to store the information from each day each row extraction
df = pd.DataFrame(columns=["Msg_Date", "Ward", "Class", "BIS", "Inflight", "BOR", "rep_index"])

# define a list to record the date of each message is recorded
# since each day has two or more messages, the 1st import rep_index will set as 0, 2nd import will set as 1
mail_date_list = []

# Iterate through every email
for i, _ in enumerate(email_list):
    msg = outlook.OpenSharedItem(os.path.join(folder_path, email_list[i]))
    # the email has been imported as a very long string
    mail = msg.Body
    mail_date = msg.SentOn
    # print(mail_date)
    # this section is to set the 1st import rep_index will set as 0, 2nd as 1
    check_rep_date = mail_date.date()
    # print(check_rep_date)
    if check_rep_date in mail_date_list:
        mail_repeat_index = 1
    else:
        mail_date_list.insert(0, check_rep_date)
        mail_repeat_index = 0

# string position of message start
    start = mail.find("Ward 2")
    mail = mail[start:]
# record the line number of each email import during the below iteration
    line_in_each_mail = 1
# iterate through every string segment of the email message
    for key in keywords:
        end = mail.find("%")  # using % sign to separate each string segment for processing
        strr = mail[:end + 1].replace('\n', '\t').replace('\r', '\t')
        str_in_list = strr.split()  # create a list based on the space separator from each segment
        # print(str_in_list)
        length = len(str_in_list)
        # print(length)
        # print(str_in_list[length-3:length])
        bis_inflight_BOR = str_in_list[length - 3:length]  # take last 3 element which is bis, inflight and BOR
        cls = [s for s in str_in_list if (s in cls_list)]  # based on cls_list keyboard to search ward class element

# ward name is obtained by first searching the ward name keyword list (from above defined list)
# in each string segment, if keyword not found use the previous stored temp ward from above string segment
        ward = ward_name(str_in_list, ward_key_word)
        if ward:
            temp_ward = ward
        else:
            ward = temp_ward

# source email message cannot consistently parse the 3nd line ward name, hence fix it as ward 3.
        if line_in_each_mail == 3:
            ward = 'Ward 3'
# using try / except to skip those null list happen due to source data table inconsistency
# combined each element in each string segment into a single list
        try:
            bis_inflight_BOR.insert(0, cls[0])
            bis_inflight_BOR.insert(0, ward)
            bis_inflight_BOR.insert(0, str(mail_date))
            bis_inflight_BOR.append(mail_repeat_index)
        except:
            pass
        # print(bis_inflight_BOR)
# append the list into DataFrame. avoid append the inconsistent length string
        if len(bis_inflight_BOR) == 7:
            to_append = bis_inflight_BOR
            df_length = len(df)
            df.loc[df_length] = to_append
        else:
            pass
# cut off the already processed string segment and prepare for the next segment processing
        mail = mail[end + 1:]
        line_in_each_mail += 1
    print(email_list[i], "is processed successfully")

df['Hour'] = pd.to_datetime(df.Msg_Date).dt.hour
df['Year'] = pd.to_datetime(df.Msg_Date).dt.year
df['Month'] = pd.to_datetime(df.Msg_Date).dt.month
df['Day'] = pd.to_datetime(df.Msg_Date).dt.day
df['Date'] = pd.to_datetime(df.Msg_Date, format='%Y-%m-%d')
df['Date'] = df.Date.astype(str).str.slice(0, 10)
df['unique'] = df.Ward + df.Class + df.BIS.astype(str) + df.Date.astype(str)

df.to_csv(PT.path_wip_output + 'BMU_email.csv', index=False)
#
# # reporting section for BIS
# df_bis_for_report = pd.read_csv(PT.path_wip_output + 'BMU_email.csv')
# # always extract the first import (based on rep_index)
# df_bis_for_report = df_bis_for_report.loc[df_bis_for_report['rep_index'] == 0]
# df_report_bis_summary = pd.pivot_table(df_bis_for_report, values='BIS', index=['Date'],
#                                        columns=['Class'],
#                                        aggfunc=np.sum, margins=True, margins_name='Total')
# df_report_bis_by_ward = pd.pivot_table(df_bis_for_report, values='BIS', index=['Date'],
#                                        columns=['Ward'],
#                                        aggfunc=np.sum, margins=True, margins_name='Total')
# df_report_bis_summary.to_csv(PT.path_report_output + 'BMU_email_summary_BIS.csv')
# print(df_report_bis_summary.head(100))
# print(df_report_bis_summary.shape)
# print("Summary report has been successfully generated")

