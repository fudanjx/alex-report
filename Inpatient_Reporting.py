from timeit import timeit
import pandas as pd
import numpy as np
import datetime
import path as PT
import data_prep as prep
import calendar
import xlwings as xw
from calendar import monthrange
import warnings

warnings.filterwarnings("ignore")

# find the 1st day of previous month and 1st day of 12 months ago
today = datetime.date.today()
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
lastYear = first - datetime.timedelta(days=360)
first_this_month = first.replace(day=1).strftime("%m/%d/%Y")
first_lastMonth = lastMonth.replace(day=1).strftime("%m/%d/%Y")
first_lastyear = lastYear.replace(day=1).strftime("%m/%d/%Y")
print("first date of last month: ", first_lastMonth)
print("first date of 12 month ago: ", first_lastyear)

# read source data, and add year month label
df_dc = pd.read_parquet(PT.path_wip_output + 'Combined_disch.parquet.gzip')
df_inflight = pd.read_parquet(PT.path_wip_output + 'Combined_inflight.parquet.gzip')
df_adm = pd.read_parquet(PT.path_wip_output + 'Combined_adm.parquet.gzip')
# setup moh speciality list df, prepare for later MOH report merging
df_moh_speciality = pd.read_excel(PT.path_lookup + 'Class.xlsx', sheet_name="MOH_Speciality",
                                  index_col='Moh_Clinical_Dept')

df_dc["Year"] = df_dc['Disch_Date'].dt.year
df_dc["Month"] = df_dc['Disch_Date'].dt.month
df_inflight["Year"] = df_inflight['Inflight_Date'].dt.year
df_inflight["Month"] = df_inflight['Inflight_Date'].dt.month
df_adm["Year"] = df_adm['Adm_Date'].dt.year
df_adm["Month"] = df_adm['Adm_Date'].dt.month

print("Load all inpatient database into the memory .... done")
# Generate subset df for last year statistics
df_inflight = df_inflight[df_inflight['Inflight_Date'] >= first_lastyear]
df_dc = df_dc[df_dc['Disch_Date'] >= first_lastyear]
df_adm = df_adm[df_adm['Adm_Date'] >= first_lastyear]

# Filter for inpatient only for adm and discharge report
df_dc = df_dc.loc[df_dc['Adm_Type'].str.contains('EM|SD|DI|EL|SO|TA', regex=True)]
df_dc = df_dc.loc[df_dc['Disch_Status'] != 'P']
df_adm = df_adm.loc[df_adm['Adm_Type'].str.contains('EM|SD|DI|EL|SO|TA', regex=True)]
df_adm = df_adm.loc[df_adm['Adm_Status'] != 'P']

print("Filter for last 12 months inpatient data .... done")

# customize for ChweeHuat's report
Report_start_last_mth = lastMonth.replace(day=1).strftime("%b-%Y")
df_adm_lastmonth_ChweeHuat = df_adm[df_adm['Adm_Date'] >= first_lastMonth]
df_adm_lastmonth_ChweeHuat = df_adm_lastmonth_ChweeHuat.loc[
    df_adm_lastmonth_ChweeHuat['Adm_Type'].str.contains('EM|SD|DI|EL', regex=True)]
df_adm_lastmonth_ChweeHuat = df_adm_lastmonth_ChweeHuat.loc[
    df_adm_lastmonth_ChweeHuat['Disch_Nrs_OU'].str.contains('LWASW|LWDSW|LWEDTU', regex=True) == False]
report_df_ChweeHuat = df_adm_lastmonth_ChweeHuat.loc[:, {
                                                            'Case_No', 'C', 'Adm_Date', 'Adm_Time', 'Adm_Cls',
                                                            'Adm_Nrs_OU', 'Disch_Date', 'Disch_Time', 'Current_Ward'}]
report_df_ChweeHuat = report_df_ChweeHuat.reindex(columns=[
    'Case_No', 'C', 'Adm_Date', 'Adm_Time', 'Adm_Cls', 'Adm_Nrs_OU', 'Disch_Date', 'Disch_Time', 'Current_Ward'])
report_df_ChweeHuat.to_csv(PT.path_report_output + 'ChweeHuat_monthly_adm_rpt(' + Report_start_last_mth + ').csv',
                           index=0)

print("Cheehuat's case level admission report generation .... done")

# prep with the necessary mapping. Inflight will map the class after merge of dataframe
df_dc.rename(columns={"Disch_Class": 'Class'}, inplace=True)
df_dc = prep.mapping_Trt_Cat(df_dc, PT.path_lookup)
df_dc.rename(columns={"Class": 'Disch_Class'}, inplace=True)
df_dc['ref_type_fin'] = df_dc.apply(lambda x: prep.fin_ref_hosp_inpt(x['Referring_Hospital_Text']), axis=1)

df_adm.rename(columns={"Adm_Cls": 'Class'}, inplace=True)
df_adm = prep.mapping_Trt_Cat(df_adm, PT.path_lookup)
df_adm.rename(columns={"Class": 'Adm_Cls'}, inplace=True)

# Generate subset discharge & inflight  for last month daily report
df_inflight_lastMonth = df_inflight[df_inflight['Inflight_Date'] >= first_lastMonth]
df_inflight_lastMonth["Month"] = df_inflight_lastMonth['Month'].apply(lambda x: calendar.month_abbr[x])
df_inflight_lastMonth["Date"] = df_inflight_lastMonth['Inflight_Date'].dt.day

df_dc_lastMonth = df_dc[df_dc['Disch_Date'] >= first_lastMonth]
df_dc_lastMonth["Month"] = df_dc_lastMonth['Month'].apply(lambda x: calendar.month_abbr[x])
df_dc_lastMonth["Date"] = df_dc_lastMonth['Disch_Date'].dt.day

print("Creating the subset dataframe for last month inpatient data .... done")

# find out the same day discharge
mask = df_adm['Adm_Date'] == df_adm['Disch_Date']
same_day_dc_df = df_adm.loc[mask]

# Section 1: Generate monthly patient days using inflight + same day disch data (based on admission report)
# extract same columns to merge into inflight
dc_to_inflight = pd.DataFrame()
dc_to_inflight['Pat_Name'] = same_day_dc_df['Pat_Name']
dc_to_inflight['Ward'] = same_day_dc_df['Disch_Nrs_OU']
dc_to_inflight['Dept_OU'] = same_day_dc_df['Disch_Dept_OU']
dc_to_inflight['Ext_Pat_ID'] = same_day_dc_df['Ext_Pat_ID']
dc_to_inflight['LOS'] = 1
dc_to_inflight['Diagnosis_Code'] = same_day_dc_df['Diagnosis_Code']
dc_to_inflight['Diagnosis_Desc'] = same_day_dc_df['Diagnosis_Desc']
dc_to_inflight['Age'] = same_day_dc_df['Age']
dc_to_inflight['Trt_Cat'] = same_day_dc_df['Adm_Trt_Cat']
dc_to_inflight['Sex'] = same_day_dc_df['Sex']
dc_to_inflight['Class'] = same_day_dc_df['Disch_Cls']
dc_to_inflight['Adm_Type'] = same_day_dc_df['Adm_Type']
dc_to_inflight['Case_No'] = same_day_dc_df['Case_No']
dc_to_inflight['Admit_Date'] = same_day_dc_df['Adm_Date']
dc_to_inflight['Inflight_Date'] = same_day_dc_df['Disch_Date']
dc_to_inflight['cnt'] = same_day_dc_df['cnt']
dc_to_inflight['Bed'] = same_day_dc_df['Disch_Bed']
dc_to_inflight['Attend_Phy'] = same_day_dc_df['Disch_Phy']
dc_to_inflight['Accom_Category'] = same_day_dc_df['Adm_Acmd_Cat']

print("inflight: ", df_inflight.shape)
# dc_to_inflight.to_csv(PT.path_wip_output + 'dc_sameday_inpatient.csv', index=0)
print("dc_same_day: ", dc_to_inflight.shape)

# concatenate both dataframe
df_inflight_final = pd.DataFrame()
frames = [dc_to_inflight, df_inflight]
df_inflight_final = pd.concat(frames)

print("final inflight: ", df_inflight_final.shape)

df_inflight_final["Month_abbr"] = df_inflight_final['Inflight_Date'].dt.month.apply(lambda x: calendar.month_abbr[x])
df_inflight_final = prep.mapping_Trt_Cat(df_inflight_final, PT.path_lookup)
df_inflight_final['Class_with_icu_iso'] = df_inflight_final['Class_abc']
df_inflight_final['Class_with_icu_iso'] = df_inflight_final.apply(
    lambda x: prep.pt_class_with_icu_iso(x['Class_abc'], x['Accom_Category'], x['Trt_Cat']), axis=1)
df_inflight_final['Class_icu_iso_MOH'] = df_inflight_final['Class_abc_MOH']
df_inflight_final['Class_icu_iso_MOH'] = df_inflight_final.apply(
    lambda x: prep.pt_class_with_icu_iso(x['Class_abc_MOH'], x['Accom_Category'], x['Trt_Cat']), axis=1)

df_inflight_final = df_inflight_final[(df_inflight_final['Ward'] != 'LWEDTU') & (df_inflight_final['Ward'] != 'LWASW')]

# df_inflight_final.to_csv(PT.path_wip_output + 'final_inflight.csv', index=0)
print("Merge same day discharge with inflight data to calculate the patient days .... done")
print("After removing EDTU/ASW, new dataframe size: ", df_inflight_final.shape)

# use for F10
df_inflight_lastMonth_w_dc = df_inflight_final[df_inflight_final['Inflight_Date'] >= first_lastMonth]

report_df_pt_days_acuity = pd.pivot_table(df_inflight_final, values='cnt', index=['Acuity', 'Dept_Name'],
                                          columns=['Year', 'Month'],
                                          aggfunc=np.sum, margins=True, margins_name='Total')

# Generate the data for lodgers by patient days (F03A). first clean up the acc cat, remap the OTHERS to the ward class
# also re-map the empty acc cat (may due to system error) to ward class
df_inflight_lodger = df_inflight_final.loc[df_inflight_final['Inflight_Date'] >= first_lastMonth]
df_ward_cls = pd.read_excel(PT.path_lookup + 'Class.xlsx', sheet_name="Ward_cls")  # find out the class of the ward
df_inflight_lodger = pd.merge(df_inflight_lodger, df_ward_cls, how='left', on='Ward')  # find out the class of the ward
df_inflight_lodger['Accom_Category'] = df_inflight_lodger['Accom_Category'].replace(['OTHER'], np.nan)
df_inflight_lodger['Accom_Category'] = df_inflight_lodger['Accom_Category'].fillna(df_inflight_lodger['Ward_cls'])
df_inflight_lodger = df_inflight_lodger.loc[df_inflight_lodger['Accom_Category'].str.contains('A1|B1|B2', regex=True)]
df_inflight_lodger = df_inflight_lodger.loc[df_inflight_lodger['Class_abc'].str.contains('B1|B2|C', regex=True)]
df_inflight_lodger = df_inflight_lodger.loc[df_inflight_lodger['Accom_Category'] != df_inflight_lodger['Class_abc']]

report_df_lodger_pt_days = pd.pivot_table(df_inflight_lodger, values='cnt', index=['Moh_Clinical_Dept'],
                                          columns=['Year', 'Month', 'Class_abc', 'Accom_Category'],
                                          aggfunc=np.sum, margins=True, margins_name='Total')
report_df_lodger_pt_days = pd.merge(report_df_lodger_pt_days, df_moh_speciality, how='right', on='Moh_Clinical_Dept')

# Section 2: Generate admission report stats
print('Start to prepare Admission statistics  ')
df_adm['Adm_Ward'] = df_adm.apply(
    lambda x: prep.replace_with_current_ward(x['Adm_Nrs_OU'], x['Current_Ward']), axis=1)
df_adm['Adm_Type_MOH'] = df_adm.apply(
    lambda x: prep.combine_EM_EL(x['Adm_Type']), axis=1)
df_adm['Paying_Status'] = df_adm.apply(
    lambda x: prep.pt_cls_sub_paying(x['Adm_Cls']), axis=1)
df_adm['Class_with_icu_iso'] = df_adm.apply(
    lambda x: prep.pt_class_with_icu_iso(x['Wish_Cls'], x['Adm_Acmd_Cat'], x['Adm_Trt_Cat']), axis=1)
df_adm = df_adm.loc[df_adm['Adm_Ward'].str.contains('LWEDTU|LWASW|LWDSW', regex=True) == False]
df_adm.rename(columns={"Moh_Clinical_Dept(Adm)": 'Moh_Clinical_Dept'}, inplace=True)
# df_adm.to_csv(PT.path_wip_output + 'temp_adm.csv', index=0)

# Generate F09 admission section

df_adm_lastmonth_F09 = df_adm[df_adm['Adm_Date'] >= first_lastMonth]
report_df_F09_adm = pd.pivot_table(df_adm_lastmonth_F09[df_adm_lastmonth_F09['Adm_Date'] >= first_lastMonth]
                                   , values='cnt', index=['Moh_Clinical_Dept'],
                                   columns=['Year', 'Month', 'Resident_Type', 'Class_with_icu_iso'],
                                   aggfunc=np.sum, margins=True, margins_name='Total')

report_df_F09_adm = pd.merge(report_df_F09_adm, df_moh_speciality, how='right', on='Moh_Clinical_Dept')

# for MOH F03
df_adm_lodger = df_adm_lastmonth_F09.loc[df_adm_lastmonth_F09['Wish_Cls'].str.contains('B2|C', regex=True)]
df_adm_lodger = df_adm_lodger[df_adm_lodger['Adm_Acmd_Cat'].notna()]

df_adm_lodger = df_adm_lodger.loc[df_adm_lodger['Adm_Acmd_Cat'].str.contains('A1|B1|B2', regex=True)]
df_adm_lodger = df_adm_lodger.loc[df_adm_lodger['Adm_Acmd_Cat'] != df_adm_lodger['Wish_Cls']]
# df_adm_lodger.to_csv(PT.path_wip_output + 'temp_adm_lodger.csv', index=0)
df_adm_lodger['Moh_Clinical_Dept'] = df_adm_lodger['Moh_Clinical_Dept'].fillna(
    df_adm_lodger['Moh_Clinical_Dept(Disch)'])
# df_adm_lodger = df_adm_lodger.loc[df_adm_lodger['Adm_Date'] >= first_lastyear]

report_df_adm_by_paying = pd.pivot_table(df_adm, values='cnt', index=['Paying_Status'], columns=['Year', 'Month'],
                                         aggfunc=np.sum, margins=True, margins_name='Total')
report_df_adm_by_ward = pd.pivot_table(df_adm, values='cnt', index=['Adm_Ward'], columns=['Year', 'Month'],
                                       aggfunc=np.sum, margins=True, margins_name='Total')
report_df_lodger_adm = pd.pivot_table(df_adm_lodger, values='cnt', index=['Moh_Clinical_Dept'],
                                      columns=['Year', 'Month', 'Adm_Type_MOH', 'Wish_Cls', 'Adm_Acmd_Cat'],
                                      aggfunc=np.sum, margins=True, margins_name='Total')
report_df_lodger_adm = pd.merge(report_df_lodger_adm, df_moh_speciality, how='right', on='Moh_Clinical_Dept')

print("Generated the lodger report based on admission and patient days ..... done")
# df_adm.to_csv(PT.path_wip_output + 'temp_adm.csv', index=0)
# df_adm_lodger.to_csv(PT.path_wip_output + 'temp_adm_lodger.csv', index=0)
# patient days report - summarize to generate results

# Section 3: Generate Discharge report stats

# df_dc['Class_with_icu_iso'] = df_dc.apply(
#     lambda x: prep.pt_class_with_icu_iso(x['Class_abc'], x['Adm_Acmd_Cat'], x['Adm_Trt_Cat']), axis=1)
df_dc = df_dc.loc[df_dc['Nrs_OU'].str.contains('LWEDTU|LWASW|LWDSW', regex=True) == False]
df_dc['cls_icu_iso'] = df_dc.apply(
    lambda x: prep.pt_cls_icu_iso_for_disch(x['Class_abc'], x['Nrs_OU'], x['Trt_Cat']), axis=1)
df_dc['death'] = df_dc.apply(lambda x: prep.death_indicator(x['Discharge_Type_Text']), axis=1)
# df_dc.to_csv(PT.path_wip_output + 'temp_disch.csv', index=0)
print('Applied the necessary filter procedure to discharge data ....... done  ')

report_df_disch_by_ward = pd.pivot_table(df_dc, values='cnt', index=['Nrs_OU'], columns=['Year', 'Month'],
                                         aggfunc=np.sum, margins=True, margins_name='Total')

# for MOH report, only take last month data
df_dc_lastmth_F09 = df_dc[df_dc['Adm_Date'] >= first_lastMonth]
report_df_F09_disch = pd.pivot_table(df_dc_lastmth_F09, values='cnt', index=['Moh_Clinical_Dept'],
                                     columns=['Year', 'Month', 'Resident_Type', 'cls_icu_iso'],
                                     aggfunc=np.sum, margins=True, margins_name='Total')
report_df_F09_disch = pd.merge(report_df_F09_disch, df_moh_speciality, how='right', on='Moh_Clinical_Dept')

report_df_F09_disch_death = pd.pivot_table(df_dc_lastmth_F09, values='death', index=['Moh_Clinical_Dept'],
                                           columns=['Year', 'Month', 'Resident_Type'],
                                           aggfunc=np.sum, margins=True, margins_name='Total')
report_df_F09_disch_death = pd.merge(report_df_F09_disch_death, df_moh_speciality, how='right', on='Moh_Clinical_Dept')

df_dc_excl_24 = df_dc.loc[df_dc['Discharge_w_in_24_hrs'] != 'X']
report_df_disch_type = pd.pivot_table(df_dc_excl_24, values='cnt', index=['Discharge_Type_Text'],
                                      columns=['Year', 'Month'],
                                      aggfunc=np.sum, margins=True, margins_name='Total')

df_dc['Discharge_w_in_24_hrs'] = df_dc['Discharge_w_in_24_hrs'].replace(['X'], 1)
report_df_disch_w_24h = pd.pivot_table(df_dc, values='Discharge_w_in_24_hrs', index=['Nrs_OU'],
                                       columns=['Year', 'Month'],
                                       aggfunc=np.sum, margins=True, margins_name='Total')

report_df_daily_pt_days = pd.pivot_table(df_inflight_lastMonth, values='cnt', index=['Ward'],
                                         columns=['Year', 'Month', 'Date'],
                                         aggfunc=np.sum, margins=True, margins_name='Total')
report_df_daily_disch = pd.pivot_table(df_dc_lastMonth, values='cnt', index=['Nrs_OU'],
                                       columns=['Year', 'Month', 'Date'],
                                       aggfunc=np.sum, margins=True, margins_name='Total')

report_df_fin_pt_days_abc = pd.pivot_table(df_inflight_final, values='cnt', index=['Program', 'Dept_Name', 'Class_abc'],
                                           columns=['Year', 'Month'],
                                           aggfunc=np.sum, margins=True, margins_name='Total')

report_df_fin_pt_days_w_iso_HD = pd.pivot_table(df_inflight_final, values='cnt',
                                                index=['Program', 'Dept_Name', 'Class_with_icu_iso'],
                                                columns=['Year', 'Month'],
                                                aggfunc=np.sum, margins=True, margins_name='Total')

report_df_F10_pt_days = pd.pivot_table(df_inflight_lastMonth_w_dc, values='cnt',
                                       index=['Moh_Clinical_Dept'],
                                       columns=['Year', 'Month', 'Resident_Type', 'Class_icu_iso_MOH'],
                                       aggfunc=np.sum, margins=True, margins_name='Total')

report_df_fin_pt_days_dept = pd.pivot_table(df_inflight_final, values='cnt',
                                            index=['Program', 'Dept_Name'],
                                            columns=['Year', 'Month'],
                                            aggfunc=np.sum, margins=True, margins_name='Total')

report_df_pt_days_by_ward = pd.pivot_table(df_inflight_final, values='cnt',
                                           index=['Ward'],
                                           columns=['Year', 'Month'],
                                           aggfunc=np.sum, margins=True, margins_name='Total')

report_df_fin_disch_abc = pd.pivot_table(df_dc, values='cnt',
                                         index=['Program', 'Dept_Name', 'Class_abc'],
                                         columns=['Year', 'Month'],
                                         aggfunc=np.sum, margins=True, margins_name='Total')

report_df_fin_disch_w_iso_HD = pd.pivot_table(df_dc, values='cnt',
                                              index=['Program', 'Dept_Name', 'cls_icu_iso'],
                                              columns=['Year', 'Month'],
                                              aggfunc=np.sum, margins=True, margins_name='Total')

report_df_fin_disch_dept = pd.pivot_table(df_dc, values='cnt',
                                          index=['Program', 'Dept_Name'],
                                          columns=['Year', 'Month'],
                                          aggfunc=np.sum, margins=True, margins_name='Total')

report_df_fin_disch_ref_type = pd.pivot_table(df_dc, values='cnt',
                                              index=['Program', 'Dept_Name', 'ref_type_fin'],
                                              columns=['Year', 'Month'],
                                              aggfunc=np.sum, margins=True, margins_name='Total')

print('Prepare all reports dataframe  ....... done  ')

# reporting section for BIS
df_bis_for_report = pd.read_csv(PT.path_wip_output + 'BMU_email.csv')
# always extract the first import (based on rep_index)
df_bis_for_report = df_bis_for_report.loc[df_bis_for_report['rep_index'] == 0]
# take note the double square bracket below
df_bis_for_report['pd_date'] = pd.to_datetime(df_bis_for_report[['Year', 'Month', 'Day']])
df_bis_for_report = df_bis_for_report[df_bis_for_report['pd_date'] >= first_lastyear]
df_bis_for_report = df_bis_for_report[df_bis_for_report['pd_date'] < first_this_month]
df_nrs_ou = pd.read_excel(PT.path_lookup + 'Class.xlsx', sheet_name="Nrs_OU",
                          index_col='Ward')
df_bis_for_report = pd.merge(df_bis_for_report, df_nrs_ou, how='left', on='Ward')
report_df_bis_by_class = pd.pivot_table(df_bis_for_report, values='BIS', index=['Class'],
                                        columns=['Year', 'Month'],
                                        aggfunc=np.sum, margins=True, margins_name='Total')
report_df_bis_by_ward = pd.pivot_table(df_bis_for_report, values='BIS', index=['Nrs_OU'],
                                       columns=['Year', 'Month'],
                                       aggfunc=np.sum, margins=True, margins_name='Total')
report_df_bis_by_class_avg = pd.pivot_table(df_bis_for_report, values='BIS', index=['Class'],
                                            columns=['Year', 'Month'],
                                            aggfunc=np.sum).round(decimals=0)
report_df_bis_by_ward_avg = pd.pivot_table(df_bis_for_report, values='BIS', index=['Nrs_OU'],
                                           columns=['Year', 'Month'],
                                           aggfunc=np.sum).round(decimals=0)

# Gives number of days in the month and divides each column by those days
for i in report_df_bis_by_class_avg.columns:
    report_df_bis_by_class_avg[i] = report_df_bis_by_class_avg[i] / monthrange(i[0], i[1])[1]

for i in report_df_bis_by_ward_avg.columns:
    report_df_bis_by_ward_avg[i] = report_df_bis_by_ward_avg[i] / monthrange(i[0], i[1])[1]

report_df_bis_by_ward_avg = np.round(report_df_bis_by_ward_avg, 0)
report_df_bis_by_class_avg = np.round(report_df_bis_by_class_avg, 0)

# adding sum rows at the end of the table
report_df_bis_by_ward_avg.loc[len(report_df_bis_by_ward_avg)] = report_df_bis_by_ward_avg.apply(np.sum).to_list()
report_df_bis_by_ward_avg = report_df_bis_by_ward_avg.rename(index={len(report_df_bis_by_ward_avg)-1: 'TOTAL'})

report_df_bis_by_class_avg.loc[len(report_df_bis_by_class_avg)] = report_df_bis_by_class_avg.apply(np.sum).to_list()
report_df_bis_by_class_avg = report_df_bis_by_class_avg.rename(index={len(report_df_bis_by_class_avg)-1: 'TOTAL'})

print('Processing BIS information ....... done  ')

# Derive the BOR DataFrame
report_df_BOR_by_ward = report_df_pt_days_by_ward / report_df_bis_by_ward

# Derive the ALOS DataFrame
report_df_ALOS_by_ward = report_df_pt_days_by_ward / report_df_disch_by_ward
report_df_ALOS_by_dept_cls = report_df_fin_pt_days_abc / report_df_fin_disch_abc
report_df_ALOS_by_dept = report_df_fin_pt_days_dept / report_df_fin_disch_dept
# round the decimal
report_df_ALOS_by_ward = report_df_ALOS_by_ward.round(decimals=1)
report_df_ALOS_by_dept_cls = report_df_ALOS_by_dept_cls.round(decimals=1)
report_df_ALOS_by_dept = report_df_ALOS_by_dept.round(decimals=1)

# start of report writer
writer = pd.ExcelWriter(PT.path_report_output + 'Inpt_rpt(' + Report_start_last_mth + ').xlsx', engine='xlsxwriter')

report_df_adm_by_ward.to_excel(writer, sheet_name='adm_by_ward')
report_df_adm_by_paying.to_excel(writer, sheet_name='adm_by_paying')
report_df_ALOS_by_ward.to_excel(writer, sheet_name='ALOS_by_ward', float_format="%0.2f")
report_df_BOR_by_ward.to_excel(writer, sheet_name='BOR_by_ward', float_format="%0.2f")
report_df_disch_by_ward.to_excel(writer, sheet_name='disch_by_ward')
report_df_pt_days_by_ward.to_excel(writer, sheet_name='pt_days_by_ward')
report_df_pt_days_acuity.to_excel(writer, sheet_name='pt_days_by_acuity')
report_df_daily_disch.to_excel(writer, sheet_name='daily_disch')
report_df_daily_pt_days.to_excel(writer, sheet_name='daily_pt_days')
report_df_disch_type.to_excel(writer, sheet_name='disch_by_DischType')
report_df_disch_w_24h.to_excel(writer, sheet_name='disch_w_24h')
report_df_bis_by_ward.to_excel(writer, sheet_name='Bed_days_by_ward')
report_df_bis_by_class.to_excel(writer, sheet_name='Bed_days_by_class')
report_df_bis_by_ward_avg.to_excel(writer, sheet_name='BIS_by_ward')
report_df_bis_by_class_avg.to_excel(writer, sheet_name='BIS_by_class')
report_df_F09_adm.to_excel(writer, sheet_name='MOH_F09_Adm')
report_df_F09_disch.to_excel(writer, sheet_name='MOH_F09_Disch')
report_df_F09_disch_death.to_excel(writer, sheet_name='MOH_F09_death')
report_df_F10_pt_days.to_excel(writer, sheet_name='MOH_F10_ptdays')
report_df_lodger_adm.to_excel(writer, sheet_name='MOH_F03_Lodger')
report_df_lodger_pt_days.to_excel(writer, sheet_name='MOH_F03a_Lodger')
report_df_fin_disch_abc.to_excel(writer, sheet_name='Fin_disch_abc')
report_df_fin_disch_dept.to_excel(writer, sheet_name='Fin_disch_dept')
report_df_fin_disch_ref_type.to_excel(writer, sheet_name='Fin_disch_ref_type')
report_df_fin_disch_w_iso_HD.to_excel(writer, sheet_name='Fin_disch_iso_icu')
report_df_fin_pt_days_abc.to_excel(writer, sheet_name='Fin_pt_days_abc')
report_df_fin_pt_days_dept.to_excel(writer, sheet_name='Fin_pt_days_dept')
report_df_fin_pt_days_w_iso_HD.to_excel(writer, sheet_name='Fin_pt_days_dept_ICU_ISO')
report_df_ALOS_by_dept.to_excel(writer, sheet_name='ALOS_by_dept', float_format="%0.2f")
report_df_ALOS_by_dept_cls.to_excel(writer, sheet_name='ALOS_by_dept&cls', float_format="%0.2f")

writer.save()
print("Reports exported, running formatting procedure ... ")
# Open a template file with Xlwings
# Open HIM Monthly Template
app = xw.App(visible=False)
template = xw.Book(PT.path_lookup + 'Inpt_rpt_format_template.xlsm')
report = xw.Book(PT.path_report_output + 'Inpt_rpt(' + Report_start_last_mth + ').xlsx')
VBA_to_format = xw.Book(PT.path_lookup + 'VBA_format_inpt.xlsm')
formatReport = VBA_to_format.macro('FormatReport')
formatReport_s = VBA_to_format.macro('FormatReport_single')

Report_start = lastYear.replace(day=1).strftime("%b-%Y").upper()
Report_end = lastMonth.replace(day=1).strftime("%b-%Y").upper()
template.sheets[0]['A2'].value = "REPORTING PERIOD FROM " + Report_start + ' TO ' + Report_end
# Format report using excel macro
for i in range(len(report.sheets)):
    if i < 21:
        formatReport_s(report.sheets[i])
    else:
        formatReport(report.sheets[i])
    report.sheets[i]['A1:AG100'].copy()
    template.sheets[i + 1]['A4'].paste()
    report.sheets[i]['Z2000'].copy()
    template.sheets[i + 1]['A3'].paste()

template.sheets[0].activate()

# Save file
template.save(PT.path_report_output + 'Inpt_rpt(' + Report_start_last_mth + ')formatted.xlsx')
template.close()
report.close()
VBA_to_format.close()
app.quit()

print("All reports successfully Generated")
