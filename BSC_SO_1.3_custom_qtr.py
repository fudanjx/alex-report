import pandas as pd
import path as PT
import data_prep as prep
import warnings
from datetime import datetime
import math
from dateutil.relativedelta import relativedelta
warnings.filterwarnings("ignore")


def obtain_new_patient(df_look_back, df_BSC_quarter):
    list_look_back = set(df_look_back.Ext_Pat_ID.to_list())
    list_BSC_quarter = set(df_BSC_quarter.Ext_Pat_ID.to_list())
    BSC_final = list_BSC_quarter - list_look_back
    return len(BSC_final)

try:
    [lastYear, lastMonth] = prep.report_dates_enter_validation()
except:
    quit()

first_lastyear = lastYear.replace(day=1).strftime("%m/%d/%Y")
start_of_quarter = datetime(year=lastMonth.replace(day=1).year,
                            month=((math.floor(((lastMonth.replace(day=1).month - 1) / 3) + 1) - 1) * 3) + 1, day=1)
end_of_quarter = start_of_quarter + relativedelta(months=3, seconds=-1)
quarter_start = start_of_quarter.strftime("%m/%d/%Y")
quarter_end = end_of_quarter.strftime("%m/%d/%Y")

print("BSC quarter start with (mm/dd/yyyy): ", quarter_start)
print("BSC quarter end with (mm/dd/yyyy): ", quarter_end)
print("Look back start with (mm/dd/yyyy): ", first_lastyear)

# for Inpatient calculation
df_adm = pd.read_parquet(PT.path_wip_output + 'Combined_adm.parquet.gzip')
df_adm["Year"] = df_adm['Adm_Date'].dt.year
df_adm["Month"] = df_adm['Adm_Date'].dt.month
df_adm = df_adm.loc[df_adm['Adm_Type'].str.contains('EM|SD|DI|EL|SO|TA', regex=True)]
df_adm = df_adm.loc[df_adm['Adm_Status'] != 'P']

df_program = pd.read_excel(PT.path_lookup + 'Class.xlsx', sheet_name="Program")
df_program.rename(columns={'Dept_OU': 'Adm_Dept_OU'}, inplace=True)
df_adm = pd.merge(df_adm, df_program, how='left', on='Adm_Dept_OU')
df_adm['cnt'] = 1

df_adm_look_back = df_adm[df_adm['Adm_Date'] >= first_lastyear]
df_adm_look_back = df_adm_look_back[df_adm['Adm_Date'] < quarter_start]

df_adm_BSC_quarter = df_adm[df_adm['Adm_Date'] <= quarter_end]
df_adm_BSC_quarter = df_adm_BSC_quarter[df_adm['Adm_Date'] >= quarter_start]

df_adm_look_back_FAST = df_adm_look_back.loc[df_adm_look_back['Program'] == 'Alex Fast Program']
df_adm_BSC_quarter_FAST = df_adm_BSC_quarter.loc[df_adm_BSC_quarter['Program'] == 'Alex Fast Program']

df_adm_look_back_HA = df_adm_look_back.loc[df_adm_look_back['Program'] == 'Alex Healthy Aging Program']
df_adm_BSC_quarter_HA = df_adm_BSC_quarter.loc[df_adm_BSC_quarter['Program'] == 'Alex Healthy Aging Program']

df_adm_look_back_PAL = df_adm_look_back.loc[df_adm_look_back['Program'] == 'Alex Palliative Program']
df_adm_BSC_quarter_PAL = df_adm_BSC_quarter.loc[df_adm_BSC_quarter['Program'] == 'Alex Palliative Program']

print('Inpatient FAST BSC1.3 for Quarter (', quarter_start, 'to', quarter_end, '):',
      obtain_new_patient(df_adm_look_back_FAST, df_adm_BSC_quarter_FAST))

print('Inpatient HA BSC1.3 for Quarter (', quarter_start, 'to', quarter_end, '):',
      obtain_new_patient(df_adm_look_back_HA, df_adm_BSC_quarter_HA))

print('Inpatient PAL BSC1.3 for Quarter (', quarter_start, 'to', quarter_end, '):',
      obtain_new_patient(df_adm_look_back_PAL, df_adm_BSC_quarter_PAL))

# for Outpatient calculation
df_SOC = pd.read_parquet(PT.path_wip_output + 'Combined_SOC.parquet.gzip')
df_SOC["Year"] = df_SOC['Visit_Date'].dt.year
df_SOC["Month"] = df_SOC['Visit_Date'].dt.month
df_SOC = df_SOC.loc[df_SOC['Visit_Type'].str.contains('FV|RV|FW|RW|DF|DR|FD|RD', regex=True)]
df_SOC = df_SOC.loc[df_SOC['Trt_Cat'] != 'NC']
df_SOC = df_SOC.loc[df_SOC['Status'] != 'P']

df_SOC_look_back = df_SOC[df_SOC['Visit_Date'] >= first_lastyear]
df_SOC_look_back = df_SOC_look_back[df_SOC['Visit_Date'] < quarter_start]

df_SOC_BSC_quarter = df_SOC[df_SOC['Visit_Date'] <= quarter_end]
df_SOC_BSC_quarter = df_SOC_BSC_quarter[df_SOC['Visit_Date'] >= quarter_start]
df_SOC_BSC_quarter = df_SOC_BSC_quarter.loc[df_SOC_BSC_quarter['Visit_Type'].str.contains('FV|FW', regex=True)]

df_SOC_look_back_Chronic = df_SOC_look_back.loc[df_SOC_look_back['Clinical_Dept'] == 'Alex Chronic Program']
df_SOC_BSC_quarter_Chronic = df_SOC_BSC_quarter.loc[df_SOC_BSC_quarter['Clinical_Dept'] == 'Alex Chronic Program']

df_SOC_look_back_HA = df_SOC_look_back.loc[df_SOC_look_back['Clinical_Dept'] == 'Alex Healthy Aging Program']
df_SOC_BSC_quarter_HA = df_SOC_BSC_quarter.loc[df_SOC_BSC_quarter['Clinical_Dept'] == 'Alex Healthy Aging Program']

print('Outpatient Chronic BSC1.3 for Quarter (', quarter_start, 'to', quarter_end, '):',
      obtain_new_patient(df_SOC_look_back_Chronic, df_SOC_BSC_quarter_Chronic))

print('Outpatient HA BSC1.3 for Quarter (', quarter_start, 'to', quarter_end, '):',
      obtain_new_patient(df_SOC_look_back_HA, df_SOC_BSC_quarter_HA))
