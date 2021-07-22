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


# Opens dialog to get directory of chosen file
def filename(reportType):
    import tkinter as tk
    from tkinter import filedialog
    root = tk.Tk()
    filepath = filedialog.askopenfilename(title=('Select a report to read (%s):' % reportType))
    root.destroy()
    return filepath


print('Processing dataframes...')
# Take only data for 1 year period
today = datetime.date.today()
first = today.replace(day=1)
lastMonthEnd = first - datetime.timedelta(days=1)
lastMonthFirst = lastMonthEnd.replace(day=1).strftime('%Y-%m-%d')
lastYear = first - datetime.timedelta(days=360)
prev_monthend = lastMonthEnd.strftime('%Y-%m-%d')
first_one_year_ago = lastYear.replace(day=1).strftime('%Y-%m-%d')
reportMonth = lastMonthEnd.strftime('%b-%Y')

############################################################################
#                            PROCEDURES
############################################################################

df_Procedures = pd.read_parquet(path_wip_output + 'Combined_procedure.parquet.gzip')

#######################################################################
# Main Dataframe Prep Work
#######################################################################

# Convert operation date to datetime object
df_Procedures['Operation_Date'] = pd.to_datetime(df_Procedures['Operation_Date'], format='%Y-%m-%d')
df_Procedures['OT_Begin_Time'] = pd.to_datetime(df_Procedures['OT_Begin_Time'], format='%H:%M:%S')  # .dt.time
df_Procedures['OT_End_Time'] = pd.to_datetime(df_Procedures['OT_End_Time'], format='%H:%M:%S')  # .dt.time

# Take only data for 1 year period
df_Procedures = df_Procedures[df_Procedures['Operation_Date'] >= first_one_year_ago]
df_Procedures["Year"] = df_Procedures['Operation_Date'].dt.year
df_Procedures["Month_Number"] = df_Procedures['Operation_Date'].dt.month
df_Procedures["Month"] = df_Procedures['Month_Number'].apply(lambda x: calendar.month_abbr[x])

# Convert OpTable to Numbers and Minor Surgical Procedures
df_Procedures['OpTable'] = df_Procedures['OpTable'].astype(str).str[0]
df_Procedures.loc[df_Procedures['OpTable'] == 'M', 'OpTable'] = 'Minor Surgical Procedures'

# Map class (Private/Subsized to Class in Procedure DF)
df_pt_cls = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name="pt_class_abc")
df_Procedures = pd.merge(df_Procedures, df_pt_cls, how='left', left_on='Cls', right_on='Class')
df_Procedures = df_Procedures.rename({'Class_abc': 'Pat_Class'}, axis='columns')
df_Procedures.loc[df_Procedures['Pat_Class'] == 'A1', 'Pat_Class'] = 'Private'

# Create extra column to harmonize Sub-Specs
# Mapping done within code below
df_Procedures['Sub-Specialty_Final'] = df_Procedures['Sub-Specialty']
df_Procedures.loc[df_Procedures['Sub-Specialty_Final'].isin(
    ['Alex Fast General Surgery', 'Alex Chronic General Surgery']), 'Sub-Specialty_Final'] = 'Alex General Surgery'
df_Procedures.loc[df_Procedures['Sub-Specialty_Final'].isin(
    ['Alex HA General Orthopaedic', 'Alex HA Adult Reconstruction']), 'Sub-Specialty_Final'] = 'Alex Orthopaedic'
# df_Procedures.loc[df_Procedures['Sub-Specialty_Final'].isin(['Alex Fast Medicine','Alex Chronic']),
# 'Sub-Specialty_Final'] = 'Alex Fast Med/Chronic'
df_Procedures.loc[df_Procedures['Sub-Specialty_Final'].isin(
    ['Alex Fast General Surgery', 'Alex Chronic General Surgery']), 'Sub-Specialty_Final'] = 'Alex General Surgery'

# Create new column for new primary key of surgeon-(sub-spec)
df_Procedures['Surgeon_Spec'] = df_Procedures['Surgeon'] + df_Procedures['Sub-Specialty_Final']

# Rename Cls to Class
df_Procedures.rename({'Cls': 'Pat_Class'}, axis='columns')

#######################################################################
# Subset of Procedure dataframe to get Day Surgeries Only
#######################################################################

# Filter dataframe by admit type
df_Procedures_OP = df_Procedures[df_Procedures['Adm_Type'].isin(['DS', 'ES', 'DO'])]

#######################################################################
# Subset of Procedure dataframe to get unique case numbers (count episodes)
#######################################################################

# Filter to keep unique case numbers from Day Surgery Subset (Episodes)
df_Procedures_OP_Unique = df_Procedures_OP.drop_duplicates(subset=['Case_No'])

#######################################################################
# Subset of Procedure dataframe to get only procedures done in OTs
#######################################################################

# Filter dataframe by admit type (for OT Utilization exclude DO and ES)
# Filter dataframe by OT (DSOT and MOT this should remove the DO and ES Admit Type)
df_Procedures_OT = df_Procedures[
    df_Procedures['Treatment_OU'].isin(['Alex Day Surgery OT', 'Alex Main Operating Theatre'])]
df_Procedures_OT = df_Procedures_OT[~df_Procedures_OT['Adm_Type'].isin(['DO', 'ES'])]
df_Procedures_OT = df_Procedures_OT.drop_duplicates(subset=['Case_No'])

#######################################################################
# Irregularity check for OTs by taking subset of OT Procedures in PAL/Geri/Rehab
#######################################################################

# Take subset of OT Procedures from PAL, Geri and Rehab as irregularity check
df_OT_Irreg = df_Procedures_OT[df_Procedures_OT['Sub-Specialty'].isin(
    ['Alex HA Rehabilitation Med', 'Alex HA Geriatric Medicine', 'Alex Palliative Care'])]

# Create new column for surgery time for each procedure
df_Procedures_OT['Surgery_Duration(Mins)'] = 0
df_Procedures_OT['Surgery_Duration(Mins)'] = (df_Procedures_OT['OT_End_Time'] - df_Procedures_OT[
    'OT_Begin_Time']) / np.timedelta64(1, 'm')

# Add 15 minutes to each procedure
df_Procedures_OT['Surgery_Duration(Mins)'] = df_Procedures_OT['Surgery_Duration(Mins)'] + 15

#######################################################################
# Subset of Procedure dataframe to get Inpatient Procedures
#######################################################################

# Take subset of dataframe to create inpatient procedures DF
df_Procedures_IP = df_Procedures[df_Procedures['Adm_Type'].isin(['DI', 'SD', 'EM', 'EL'])]

# Create pivot table for episodes
procedures_df_1 = pd.pivot_table(df_Procedures_OP, values='cnt',
                                 index=['Clinical_Dept', 'Sub-Specialty', 'Treatment_OU', 'Resident_Type', 'Pat_Class'],
                                 columns=['Year', 'Month_Number', 'Month'],
                                 aggfunc=np.sum, margins=True, margins_name='Total')

procedures_df_2 = pd.pivot_table(df_Procedures_OP_Unique, values='cnt',
                                 index=['Clinical_Dept', 'Sub-Specialty', 'Treatment_OU', 'Resident_Type', 'Pat_Class'],
                                 columns=['Year', 'Month_Number', 'Month'],
                                 aggfunc=np.sum, margins=True, margins_name='Total')

procedures_df_3 = pd.pivot_table(df_Procedures_OT, values='Surgery_Duration(Mins)',
                                 index=['Treatment_OU', 'Sub-Specialty_Final', 'Treatment_Rm'],
                                 columns=['Year', 'Month_Number', 'Month'],
                                 aggfunc=np.sum, margins=True, margins_name='Total')

procedures_df_4 = pd.pivot_table(df_Procedures_IP, values='cnt', index=['Sub-Specialty_Final', 'OpTable'],
                                 columns=['Year', 'Month_Number', 'Month'],
                                 aggfunc=np.sum, margins=True, margins_name='Total')

procedures_df_5 = pd.pivot_table(df_Procedures_OP, values='cnt', index=['Sub-Specialty_Final', 'OpTable'],
                                 columns=['Year', 'Month_Number', 'Month'],
                                 aggfunc=np.sum, margins=True, margins_name='Total')

procedures_df_6 = pd.pivot_table(df_Procedures_IP, values='cnt', index=['Surgeon', 'Surgeon_MCR_No'],
                                 columns=['Year', 'Month_Number', 'Month'],
                                 aggfunc=np.sum, margins=True, margins_name='Total')

procedures_df_7 = pd.pivot_table(df_Procedures_OP, values='cnt', index=['Surgeon', 'Surgeon_MCR_No'],
                                 columns=['Year', 'Month_Number', 'Month'],
                                 aggfunc=np.sum, margins=True, margins_name='Total')

procedures_df_8 = pd.pivot_table(df_Procedures, values='cnt', index=['Anaesthetist_MCR_No', 'Pat_Class'],
                                 columns=['Year', 'Month_Number', 'Month'],
                                 aggfunc=np.sum, margins=True, margins_name='Total')

procedures_df_9 = pd.pivot_table(df_OT_Irreg, values='cnt',
                                 index=['Surgeon', 'Surgeon_MCR_No', 'Sub-Specialty', 'Treatment_OU'],
                                 columns=['Year', 'Month_Number', 'Month'],
                                 aggfunc=np.sum, margins=True, margins_name='Total')

# =============================================================================
# procedures_df_9_1 = df_OT_Irreg[['Operation_Date','Case_No','Sub-Specialty','Treatment_OU','Surgeon',
# 'Surgeon_MCR_No']]
# procedures_df_9_1['Operation_Date'] = pd.to_datetime(procedures_df_9_1['Operation_Date']).dt.date
# 
# procedures_df_10 = pd.pivot_table(df_Procedures, values='cnt', index=['Surgeon','Sub-Specialty_Final'],
# columns=['Year', 'Month_Number', 'Month'],
#                               aggfunc=np.sum, margins=True, margins_name='Total')
# 
# procedures_df_10_1 = pd.pivot_table(df_Procedures, values='cnt', index=['Surgeon','Sub-Specialty_Final',
# 'Surgeon_Spec'], columns=['Year', 'Month_Number', 'Month'],
#                               aggfunc=np.sum, margins=True, margins_name='Total')
# =============================================================================

#######################################################################
# Irregularity check for procedures whether done by correct surgeon
#######################################################################

# For creating irregularity list for procedures
# Create list of surgeons who had procedures in 2 different unrelated subspecs
# =============================================================================
# procedures_df_wc = procedures_df_10_1.reset_index()
# temp_df = (procedures_df_wc['Surgeon'].value_counts() > 1).reset_index()
# temp_df = temp_df[temp_df['Surgeon'] == True]
# repeat_surgeon = temp_df['index'].tolist()
# 
# #Take subset of pivot table with surgeon-subspec key for surgeons with procedures
# #in 2 unrelated subspecs
# procedures_df_wc = procedures_df_wc[procedures_df_wc['Surgeon'].isin(repeat_surgeon)]
# #Drop the rows with a lower total procedure performed to keep the sub-spec with low procedure count
# #for surgeons with procedures in 2 different subspecs
# procedures_df_wc = (procedures_df_wc.sort_values(by='Total',ascending=True))[['Surgeon',
# 'Surgeon_Spec']].drop_duplicates(subset=[(                 'Surgeon', '', '')],keep='first')
# #Create a list of surgeon-subspec keys for these respective rows
# irregList = procedures_df_wc['Surgeon_Spec'].tolist()
# 
# #Take subset of main procedure dataframe with these surgeon-subspec keys
# df_Procedures_Irreg = df_Procedures[df_Procedures['Surgeon_Spec'].isin(irregList)]
# df_Procedures_Irreg = df_Procedures_Irreg[['Operation_Date','Case_No','Sub-Specialty','Treatment_OU','Surgeon',
# 'Surgeon_MCR_No']]
# df_Procedures_Irreg['Operation_Date'] = pd.to_datetime(df_Procedures_Irreg['Operation_Date']).dt.date
# =============================================================================


#################################################################################
#                 SOC ATTENDANCE REPORT
#################################################################################

# read source data
df_SOC = pd.read_parquet(path_wip_output + 'Combined_SOC.parquet.gzip')
# dates read in pq is read as object so reconvert it into datetime object
df_SOC['Visit_Date'] = pd.to_datetime(df_SOC['Visit_Date'])
# df_SOC = pd.read_parquet(path_wip_output + 'Combined_SOC.parquet')

# Generate subset SOC df for last year statistics and take actual
df_SOC = df_SOC[df_SOC['Status'] != 'P']
df_SOC = df_SOC[(df_SOC['Visit_Date'] >= first_one_year_ago) & (df_SOC['Visit_Date'] <= prev_monthend)]
# df_SOC_lastMonth = df_SOC[df_SOC['Visit_Date'] >= first_lastMonth]
df_SOC["Year"] = df_SOC['Visit_Date'].dt.year
df_SOC["Month_Number"] = df_SOC['Visit_Date'].dt.month
df_SOC["Month"] = df_SOC['Month_Number'].apply(lambda x: calendar.month_abbr[x])
df_SOC["Date"] = df_SOC['Visit_Date'].dt.day
# df_SOC.drop_duplicates(subset=['Case_No', 'Visit_Date', 'Visit_Time'],
#                        inplace=True)
# df_SOC.to_csv(path_wip_output + 'SOC_temp.csv')

print('\nProcessing data...')
df_SOC['Age'] = df_SOC['Age'].astype(int)
# Take only relevant Visit Types and exclude NC Treatment Cat
df_SOC = df_SOC[
    (df_SOC['Visit_Type'].isin(['FV', 'RV', 'FW', 'RW', 'DF', 'DR', 'FD', 'RD'])) & (df_SOC['Trt_Cat'] != 'NC')]

# define rehab med doctor list and use it to change Treatment OU of Rehab Med Patients to reflect Rehab Med
psychMedMCR = ['L11767F', 'L17139E', 'L05460G']
cardioMCR = ['L16082B', 'L18700C']
df_SOC.loc[(df_SOC['Attn_MCR'].isin(psychMedMCR)) & (
        df_SOC['Sub-Specialty_ID'] == 'LSCHRO'), 'Trt_OU'] = 'Psych Medicine (I-Care)'
df_SOC.loc[df_SOC['Attn_MCR'].isin(cardioMCR), 'Trt_OU'] = 'Cardiology (I-Care)'

# Create visit tagging for First / Repeat Visits
df_SOC['Visit_Tagging'] = 0
df_SOC.loc[df_SOC['Visit_Type'].isin(['FV', 'FW', 'DF', 'FD']), 'Visit_Tagging'] = 'New'
df_SOC.loc[df_SOC['Visit_Type'].isin(['RV', 'RW', 'DR', 'RD']), 'Visit_Tagging'] = 'Repeat'

# Create telehealth column
df_SOC['Telehealth_Status'] = 0
df_SOC.loc[df_SOC['Visit_Type'].isin(['DF', 'FD', 'DR', 'RD']), 'Telehealth_Status'] = 'TeleHealth'
df_SOC.loc[df_SOC['Visit_Type'].isin(['FV', 'RV', 'FW', 'RW']), 'Telehealth_Status'] = 'Not TeleHealth'

# Create Age Groups
df_SOC['Age_Group'] = '1-4'
ageFloor = 5
ageCeiling = 9
while ageCeiling < 85:
    df_SOC.loc[((df_SOC['Age'] >= ageFloor) & (df_SOC['Age'] <= ageCeiling)), 'Age_Group'] = (
            str(ageFloor) + '-' + str(ageCeiling))
    ageFloor += 5
    ageCeiling += 5
df_SOC.loc[df_SOC['Age'] < 1, 'Age_Group'] = '< 1'

# Map treatment cat to get private/sub
df_SOC_Mapped = mapping_Trt_Cat(df_SOC, path_lookup)
df_SOC_Mapped.loc[df_SOC_Mapped['Class_abc'] == 'Subsidized', 'Class_abc'] = 'Subsidised'

# Map to get referral hospital classification
df_SOC_Mapped['Ref_Hosp'] = df_SOC_Mapped.apply(lambda x: fin_ref_hosp_soc(x['Referral_type'], x['Referral_Hospital']),
                                                axis='columns')

# Not all are Private/Sub, there are some classified with Class A so grouped into Private
df_SOC_Mapped.loc[df_SOC_Mapped['Class_abc'] == 'Class A', 'Class_abc'] = 'Private'
df_SOC_Mapped.loc[df_SOC_Mapped['Class_abc'] == 'A1', 'Class_abc'] = 'Private'

# Rename Class_abc as Pat_Class
df_SOC_Mapped = df_SOC_Mapped.rename({'Class_abc': 'Pat_Class', 'Resident_Type': 'Resident'}, axis='columns')

df_SOC_Mapped_Sub = df_SOC_Mapped[df_SOC_Mapped['Visit_Date'] >= lastMonthFirst]

df_MOHF12 = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name="MOH_F12",
                          index_col=[0, 1, 2, 3, 4])

# Create pivot tables for different permutations
df_SOC1 = pd.pivot_table(df_SOC_Mapped, values='cnt',
                         index=['Visit_Tagging', 'Clinical_Dept', 'Sub-Specialty', 'Trt_OU', 'Pat_Class'],
                         columns=['Year', 'Month_Number', 'Month'],
                         aggfunc=np.sum, margins=True, margins_name='Total')

df_SOC2 = pd.pivot_table(df_SOC_Mapped, values='cnt',
                         index=['Visit_Tagging', 'Clinical_Dept', 'Sub-Specialty', 'Ref_Hosp', 'Pat_Class'],
                         columns=['Year', 'Month_Number', 'Month'],
                         aggfunc=np.sum, margins=True, margins_name='Total')

df_SOC3 = pd.pivot_table(df_SOC_Mapped, values='cnt', index=['Visit_Tagging', 'Trt_OU', 'Pat_Class'],
                         columns=['Year', 'Month_Number', 'Month'],
                         aggfunc=np.sum, margins=True, margins_name='Total')

df_SOC4 = pd.pivot_table(df_SOC_Mapped, values='cnt', index=['Trt_OU', 'Pat_Class'],
                         columns=['Year', 'Month_Number', 'Month'],
                         aggfunc=np.sum, margins=True, margins_name='Total')

df_SOC5 = pd.pivot_table(df_SOC_Mapped, values='cnt',
                         index=['Resident_MOH', 'Telehealth_Status', 'Visit_Tagging',
                                'Pat_Class'], columns=['Year', 'Month_Number', 'Month'],
                         aggfunc=np.sum, margins=True, margins_name='Total')

df_SOC5_1 = pd.merge(df_MOHF12, df_SOC5, how='left', left_index=True, right_index=True)
df_SOC5_1.columns = pd.MultiIndex.from_tuples(df_SOC5_1.columns.tolist())
df_SOC5_1.fillna(value=0, inplace=True)

df_SOC6 = pd.pivot_table(df_SOC_Mapped, values='cnt',
                         index=['Visit_Tagging', 'Telehealth_Status', 'Age_Group', 'Sex', 'Race'],
                         columns=['Year', 'Month_Number', 'Month'],
                         aggfunc=np.sum, margins=True, margins_name='Total')

df_SOC7 = pd.pivot_table(df_SOC_Mapped, values='cnt', index=['Attn_Phy', 'Attn_MCR', 'Visit_Tagging'],
                         columns=['Year', 'Month_Number', 'Month'],
                         aggfunc=np.sum, margins=True, margins_name='Total')

df_SOC8 = pd.pivot_table(df_SOC_Mapped_Sub, values='cnt',
                         index=['Visit_Tagging', 'Clinical_Dept', 'Sub-Specialty', 'Telehealth_Status'],
                         columns=['Year', 'Month_Number', 'Month', 'Referral_type', 'Referral_Hospital', 'Pat_Class'],
                         aggfunc=np.sum, margins=True, margins_name='Total')

# Take mapping from Class sheet to map referral hospitals to F04 format
df_MOHF04_Map = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name='Referral_F04')
df_MOHF04 = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name='MOH_F04', index_col=[0, 1, 2, 3])

df_SOC_Mapped = pd.merge(df_SOC_Mapped, df_MOHF04_Map, how='left', on='Referral_Hospital')
df_SOC_Mapped.loc[
    (df_SOC_Mapped['Referral_type'] == 'Direct Access G') | (df_SOC_Mapped['Referral_type'] == 'Direct Access GP') | (
            df_SOC_Mapped['Referral_type'] == 'Pte Practitione'), 'Source'] = 'GP'
df_SOC_Mapped.loc[
    (df_SOC_Mapped['Referral_type'] == 'Direct Access G') | (df_SOC_Mapped['Referral_type'] == 'Direct Access GP') | (
            df_SOC_Mapped['Referral_type'] == 'Pte Practitione'), 'Source (Specific)'] = 'Not Applicable'

df_SOC_Mapped.loc[df_SOC_Mapped['Referral_type'] == 'Pte Hospital', 'Source'] = 'Private Hospitals'
df_SOC_Mapped.loc[df_SOC_Mapped['Referral_type'] == 'Pte Hospital', 'Source (Specific)'] = 'Not Applicable'

df_SOC_Mapped.loc[df_SOC_Mapped['Referral_type'] == 'Step-Down Care', 'Source'] = 'Others'
df_SOC_Mapped.loc[df_SOC_Mapped['Referral_type'] == 'Step-Down Care', 'Source (Specific)'] = 'Not Applicable'

df_SOC9 = pd.pivot_table(df_SOC_Mapped, values='cnt',
                         index=['Source', 'Source (Specific)', 'Pat_Class', 'Telehealth_Status'],
                         columns=['Year', 'Month_Number', 'Month'],
                         aggfunc=np.sum, margins=True, margins_name='Total')

df_SOC9_1 = pd.merge(df_MOHF04, df_SOC9, how='left', left_index=True, right_index=True)
df_SOC9_1.columns = pd.MultiIndex.from_tuples(df_SOC9_1.columns.tolist())
df_SOC9_1.fillna(value=0, inplace=True)

#################################################################################
#                 UCC REPORT
#################################################################################

# Read source data
df_UCC = pd.read_parquet(path_wip_output + 'Combined_UCC.parquet.gzip')

# Convert dataframe to datetime formats
df_UCC['Visit_Date'] = pd.to_datetime(df_UCC['Visit_Date'])

df_UCC = df_UCC[df_UCC['Visit_Date'] >= first_one_year_ago]
df_UCC["Year"] = df_UCC['Visit_Date'].dt.year
df_UCC["Month_Number"] = df_UCC['Visit_Date'].dt.month
df_UCC["Month"] = df_UCC['Month_Number'].apply(lambda x: calendar.month_abbr[x])

# Remove Cancellations
df_UCC = df_UCC[df_UCC['Att_Phy_Name'] != 'CANCELLATION']

# Create pivot table for Attendance
df_UCC_1 = pd.pivot_table(df_UCC, values='cnt', index=['PACS', 'Arrival_Mode'],
                          columns=['Year', 'Month_Number', 'Month'],
                          aggfunc=np.sum, margins=True, margins_name='Total')

df_UCC_2 = pd.pivot_table(df_UCC, values='cnt', index=['PACS', 'Case_End_Type'],
                          columns=['Year', 'Month_Number', 'Month'],
                          aggfunc=np.sum, margins=True, margins_name='Total')

df_UCC_3 = pd.pivot_table(df_UCC, values='cnt', index=['Att_Phy_Name', 'Att_Phy_MCR_No'],
                          columns=['Year', 'Month_Number', 'Month'],
                          aggfunc=np.sum, margins=True, margins_name='Total')

df_UCC_4 = pd.pivot_table(df_UCC, values='cnt', index=['Pri_Diag_Code', 'Pri_Diag_Desc'],
                          columns=['Year', 'Month_Number', 'Month'],
                          aggfunc=np.sum, margins=True, margins_name='Total')

# Error Check


print('\nCreating reports...')
writer = pd.ExcelWriter(path_report_output + 'SOC_Procedures_UCC Report (' + reportMonth + ').xlsx',
                        engine='xlsxwriter')
# writer2 = pd.ExcelWriter(path_report_output + 'Procedure_IrregCheck (' + reportMonth + ').xlsx', engine='xlsxwriter')
writer3 = pd.ExcelWriter(path_report_output + 'Doctor_Workload_Report (' + reportMonth + ').xlsx', engine='xlsxwriter')
# Procedures
procedures_df_1.to_excel(writer, sheet_name='Monthly_DSSurgicalProcedure')
procedures_df_2.to_excel(writer, sheet_name='Monthly_SurgicalEpisodes')
procedures_df_3.to_excel(writer, sheet_name='Monthly_OTUtilization')
procedures_df_4.to_excel(writer, sheet_name='Monthly_IPProcedures')
procedures_df_5.to_excel(writer, sheet_name='Monthly_DSProcedures')
procedures_df_6.to_excel(writer3, sheet_name='Monthly_IPWorkload')
procedures_df_7.to_excel(writer3, sheet_name='Monthly_DSWorkload')
procedures_df_8.to_excel(writer3, sheet_name='Monthly_AnaesWorkload')
# =============================================================================
# procedures_df_9.to_excel(writer2, sheet_name='OTWorkloadIrregs')
# procedures_df_9_1.to_excel(writer2, sheet_name='OTWorkloadIrregList')
# procedures_df_10.to_excel(writer2, sheet_name='ProceduresIrreg')
# df_Procedures_Irreg.to_excel(writer2, sheet_name='ProceduresIrregList')
# =============================================================================


# SOC
df_SOC1.to_excel(writer, sheet_name='Monthly_Att_TrtOU')
df_SOC2.to_excel(writer, sheet_name='Monthly_Att_RefSource')
# df_SOC3.to_excel(writer, sheet_name='Monthly_Att_TrtOU_NoSubSpec')
# df_SOC4.to_excel(writer, sheet_name='Monthly_Att_TrtOU_Alone')
df_SOC5.to_excel(writer, sheet_name='Monthly_F12')
df_SOC6.to_excel(writer, sheet_name='Monthly_SOC_Demographics')
df_SOC7.to_excel(writer3, sheet_name='SOCDoctorWorkload')
df_SOC8.to_excel(writer, sheet_name='Monthly_SOC_RefType+Source')

# UCC
df_UCC_1.to_excel(writer, sheet_name='MonthlyAttendance')
df_UCC_2.to_excel(writer, sheet_name='MonthlyCaseEndType')
df_UCC_3.to_excel(writer3, sheet_name='DoctorWorkload')
df_UCC_4.to_excel(writer, sheet_name='AttendancebyDiagCode')

writer.save()
# writer2.save()
writer3.save()

writer4 = pd.ExcelWriter(path_report_output + 'MOH F12 Report (' + reportMonth + ').xlsx', engine='xlsxwriter')
df_SOC5_1.to_excel(writer4, sheet_name='F12')
writer4.save()

writer5 = pd.ExcelWriter(path_report_output + 'MOH F04 Report (' + reportMonth + ').xlsx', engine='xlsxwriter')
df_SOC9_1.to_excel(writer5, sheet_name='F04')
writer5.save()
print('\nReports have been generated.')

print('\nFormatting report to HIM Monthly Format.')
# Open HIM Monthly Template
app = xw.App(visible=False)
template = xw.Book(path_lookup + 'SOC_Procedures_UCC_Format.xlsx')
report = xw.Book(path_report_output + 'SOC_Procedures_UCC Report (' + reportMonth + ').xlsx')
macrobk = xw.Book(path_lookup + 'Macrobook.xlsm')
formatReport = macrobk.macro('FormatReport')

Report_start = lastYear.replace(day=1).strftime("%b-%Y").upper()
Report_end = lastMonthEnd.replace(day=1).strftime("%b-%Y").upper()
template.sheets[0]['A2'].value = "REPORTING PERIOD FROM " + Report_start + ' TO ' + Report_end

# Format report using excel macro
for i in range(len(report.sheets)):
    formatReport(report.sheets[i])

# Assign data to cells (Z2000 is arbitrary)
report.sheets[5]['A1:Z250'].copy()
template.sheets[1]['A4'].paste()
report.sheets[6]['A1:Z250'].copy()
template.sheets[2]['A4'].paste()
report.sheets[0]['A1:Z250'].copy()
template.sheets[3]['A4'].paste()
report.sheets[1]['A1:Z250'].copy()
template.sheets[4]['A4'].paste()
report.sheets[2]['A1:Z250'].copy()
template.sheets[5]['A4'].paste()
report.sheets[7]['A1:Z550'].copy()
template.sheets[6]['A4'].paste()
report.sheets[8]['A1:Z250'].copy()
template.sheets[7]['A4'].paste()
report.sheets[3]['A1:Z250'].copy()
template.sheets[8]['A4'].paste()
report.sheets[4]['A1:Z250'].copy()
template.sheets[9]['A4'].paste()
report.sheets[9]['A1:CN50'].copy()
template.sheets[10]['A4'].paste()
report.sheets[10]['A1:Z250'].copy()
template.sheets[11]['A4'].paste()
report.sheets[11]['A1:Z250'].copy()
template.sheets[12]['A4'].paste()
report.sheets[12]['A1:Z1500'].copy()
template.sheets[13]['A4'].paste()

for i in range(len(template.sheets)):
    template.sheets[i]['Z4000'].copy()
    template.sheets[i]['A3'].paste()

template.sheets[0].activate()

# Save file
template.save(path_report_output + 'HIM Report (SOC,Procedures,UCC) (' + reportMonth + ').xlsx')
template.close()
report.close()
macrobk.close()
app.quit()

# Remove initial generated report
# os.remove(path_report_output + 'SOC_Procedures_UCC Report ('+ reportMonth + ').xlsx')
print('\nHIM Report Generation fully completed.')
