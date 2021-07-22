import pandas as pd
import numpy as np
import datetime
from path import *
from data_prep import *
import calendar
import warnings
warnings.filterwarnings("ignore")
import xlwings as xw
import os

#Opens dialog to get directory of chosen file
def filename(reportType):
    import tkinter as tk
    from tkinter import filedialog
    root = tk.Tk()
    filepath = filedialog.askopenfilename(title=('Select a report to read (%s):'%reportType))
    root.destroy()
    return filepath

#Opens dialog to get directory of chosen folder/location
def filedirectory():
    import tkinter as tk
    from tkinter import filedialog
    root = tk.Tk()
    filepath = filedialog.askdirectory(title='Select a file directory to save output at: ')
    root.destroy()
    return filepath

#Read and format eQMS WT Report
eQMSexcel = filename('eQMS WT Report')
eQMS = pd.read_excel(eQMSexcel,header=5)
eQMS = eQMS.iloc[:,1:-1]
eQMS = eQMS.drop(columns=['Unnamed: 2'])
eQMS['Service Date'] = pd.to_datetime(eQMS['Service Date'],format='%d/%m/%Y').dt.strftime('%d/%m/%Y')

#Key for checking whether there are prior invesgations
eQMS['ID_Key_eQMS'] = eQMS['Service Date'] + eQMS['IC Number'] + eQMS['Queue No']

#Create a copy to maintain as master copy
eQMS_Master = eQMS.copy()

#Keep only 2 visit types
eQMS = eQMS[eQMS['Visit Type'].isin(['FV','RV'])]

#Keep only consults with appointment time stated
eQMS = eQMS[(eQMS['Service Type'] == 'Consultation') & (eQMS['Appt Time (HH:MM)'] != ' ')]

#Create new columns for appt time and wait start time for comparison (time format)
eQMS['Appt_Time'] = pd.to_datetime(eQMS['Appt Time (HH:MM)'])
eQMS['Wait_StartTime'] = pd.to_datetime(eQMS['Wait Start Time (HH:MM)'])

#Key to map back to SOC Dataframe
eQMS['ID_Key'] = eQMS['Service Date'] + eQMS['IC Number'] + eQMS['Treatment OU']

#Keep only rows where consult wait time >= appointment time
eQMS = eQMS[eQMS['Wait_StartTime'] >= eQMS['Appt_Time']]

#Convert treatment OU to Cardio if Dept OU is LSCHCACA
eQMS.loc[eQMS['Dept OU'] == 'LSCHCACA', 'Treatment OU'] = 'LSCHCACA'

specialtyList = ['LCRHM','LCAOCC','LCCGSC','LCCICARE','LCHAENT','LCFAVAS','LCGSH1',
                 'LCHAHC','LCHAJRC','LCHAOPT','LCHAURO','LCWEGYN','LSCHCACA']

specialtyCodeList = ['35','1','12','13','9','4','14','14','28','10','41','25','3']

#Construct empty dataframe with same column structure
eQMS_Consult = eQMS[0:0]
eQMS_Consult['Specialty Code'] = ''

#Take 50 samples from each specialty and all rows if specialty has less than 50 cases
#Map specialty code
for ou,ouCode in zip(specialtyList,specialtyCodeList):
    if (eQMS[eQMS['Treatment OU'] == ou].shape[0]) > 50:        
        eQMS_Consult = pd.concat([eQMS_Consult,eQMS[eQMS['Treatment OU'] == ou].sample(n=50)])
        
    else:
        eQMS_Consult = pd.concat([eQMS_Consult,eQMS[eQMS['Treatment OU'] == ou]])
    
    eQMS_Consult.loc[eQMS_Consult['Treatment OU'] == ou,'Specialty Code'] = ouCode

##Find out whether there is prior investigation
#Include investigation services
serviceInclusion = ['Audiology','BMD','Cardiology Diagnostic Tests','CT','Investigation',
                    'Mammogram','MRI','Neurology Diagnostic Tests','Pulmonary Function Tests',
                    'Refraction','Ultrasound','VA','X-ray','Consultation']

eQMS_Sub = eQMS_Master[eQMS_Master['Service Type'].isin(serviceInclusion)]

#Take only those rows in our sample and use eQMS_Sub to check whether there is investigation
#Assign prior investigation code '1' for Yes and '2' for No
eQMS_Sub = eQMS_Sub[eQMS_Sub['ID_Key_eQMS'].isin(eQMS_Consult['ID_Key_eQMS'].unique().tolist())]
eQMS_Sub = eQMS_Sub[['ID_Key_eQMS','IC Number']].groupby(['ID_Key_eQMS']).count().reset_index()
eQMS_Sub = eQMS_Sub.rename(columns={'IC Number': 'InvestigationCount'})
eQMS_Sub['Prior Investigation'] = 0
eQMS_Sub.loc[eQMS_Sub['InvestigationCount'] > 1,'Prior Investigation'] = 1
eQMS_Sub.loc[eQMS_Sub['InvestigationCount'] <= 1,'Prior Investigation'] = 2

#Join both to obtain prior investigation code for each case
eQMS_Consult = pd.merge(eQMS_Consult,eQMS_Sub,how='left',on='ID_Key_eQMS')

#Add Subsidy Code by reading SOC Dataframe and mapping back
df_SOC = pd.read_parquet(path_wip_output + 'Combined_SOC.parquet.gzip')
df_SOC = df_SOC[df_SOC['Status'] != 'P']
df_SOC['Visit_Date'] = pd.to_datetime(df_SOC['Visit_Date'],format='%d/%m/%Y').dt.strftime('%d/%m/%Y')
df_SOC['Visit_Time'] = pd.to_datetime(df_SOC['Visit_Time']).dt.strftime('%H:%M')
df_SOC['ID_Key'] = df_SOC['Visit_Date'] + df_SOC['Ext_Pat_ID'] + df_SOC['Trt_OU_ID']
df_SOC = df_SOC[['Case_No','Ext_Pat_ID','Trt_OU','ID_Key','Class']]

#Join dataframes to get Subsidy Status
eQMS_Consult = pd.merge(eQMS_Consult,df_SOC,how='left',on='ID_Key')
eQMS_Consult.loc[eQMS_Consult['Class'].isin(['SUB','SUBP']),'Class'] = 1
eQMS_Consult.loc[eQMS_Consult['Class'].isin(['PTRF','PTE','PTEP','NR']),'Class'] = 2

#Convert waiting time format to minutes and rename columns
eQMS_Consult['Wait Time (HH:MM)'] = eQMS_Consult['Wait Time (HH:MM)'].str.split(':').apply(lambda x: int(x[0])*60+int(x[1]))

eQMS_Consult = eQMS_Consult.rename(columns={'Service Date': 'Visit Date','Class':'Subsidy Code','Appt Time (HH:MM)': 'Appt Time',
                             'Check In Time (HH:MM)':'Registration Time','Wait Start Time (HH:MM)': 'Consult Wait Start',
                             'Serving Time Start (HH:MM)':'Consult Start Time','Wait Time (HH:MM)':'EQMS Wait Time'})

#Take only relevant columns
eQMS_Consult = eQMS_Consult[['Case_No','Ext_Pat_ID','Trt_OU','Visit Date','Specialty Code','Subsidy Code','Appt Time','Registration Time','Prior Investigation','Consult Wait Start','Consult Start Time','EQMS Wait Time']]

writer = pd.ExcelWriter(path_report_output + 'MOH_Quarterly F41.xlsx', engine='xlsxwriter')
eQMS_Consult.to_excel(writer, sheet_name='F41',index=False)
writer.save()
print('Report has been generated.')
print(f'Number of rows in the report is: {eQMS_Consult.shape[0]}')