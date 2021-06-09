import pandas as pd
import path as PT
import datetime
from dateutil.parser import parse

# initial commit
desired_width = 6200
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 12)

# determine how much to data to examine
today = datetime.date.today()
first = today.replace(day=1)
Start_Month = first - datetime.timedelta(days=1120)
starting_date = Start_Month.replace(day=1).strftime("%m/%d/%Y")
print("starting Date: ", starting_date)


# check whether the particular visit is within 180 days of qualified post discharge period
# need to extract the closet valid discharge date based on SOC visit date, and check for the difference
def check_case_in_soc(pt_nric, date_of_disch, soc_df):
    soc_df_search = soc_df.loc[soc_df['Ext_Pat_ID'] == pt_nric, :]
    list_of_soc_date = soc_df_search['Visit_Date'].to_list()
    date_delta = [x - date_of_disch for x in list_of_soc_date]
    try:
        closest_delta_duration = min([item for item in date_delta if item > datetime.timedelta(days=1)])
        # delta_duration = closest_value - date_of_disch
        if closest_delta_duration < datetime.timedelta(days=120):
            if closest_delta_duration > datetime.timedelta(days=1):
                return True
            else:
                return False
        else:
            return False
    except:
        pass


# Load the data source with necessary filters
df_dc = pd.read_parquet(PT.path_wip_output + 'Combined_disch.parquet.gzip',
                        columns=['Case_No', 'Ext_Pat_No', 'Dept_OU', 'Adm_Date', 'Disch_Date', 'Adm_Type',
                                 'Discharge_Physician_Name', 'Pri_Diag_Code_Text', 'Discharge_Type_Text',
                                 'Referring_Hospital_Text']).sort_values(
    by=['Disch_Date'])
df_dc = df_dc[df_dc.Dept_OU.isin(['LSHAGERI', 'LSCHRO', 'LSFAMED'])]
df_dc = df_dc[df_dc['Disch_Date'] >= starting_date]

df_soc = pd.read_parquet(PT.path_wip_output + 'Combined_SOC.parquet.gzip',
                         columns=['Case_No', 'Ext_Pat_ID', 'Sub-Specialty_ID', 'Visit_Date', 'Visit_Type',
                                  'Referral_Hospital', 'Attn_Phy', 'Comments']).sort_values(by=['Visit_Date'])
df_soc = df_soc[df_soc['Visit_Date'] >= starting_date]
df_soc = df_soc.loc[df_soc['Visit_Type'].str.contains('FV|RV', regex=True)]
df_soc = df_soc[df_soc['Sub-Specialty_ID'].isin(['LSHAGERI', 'LSCHRO'])]
df_soc = df_soc.loc[df_soc['Referral_Hospital'].str.contains('Intra-Dept referral Ward')]
df_soc = df_soc.loc[
    df_soc['Attn_Phy'].str.contains('APN|PODIATRIST|PHYSIO|PHARMACIST|PSYCHOLOGIST', regex=True) == False]

# initiate the output dataframe. If the data files is not found, initiate a new empty data frame
try:
    df_master_case_list = pd.read_csv(PT.path_wip_output + 'master_SOC_BSC_list_dc.csv',
                                      index_col='Case_ID').reset_index()
    # print(df_master_case_list)
except:
    df_master_case_list = pd.DataFrame(columns=["Disch_Date", "Ext_Pat_ID", "Case_ID"])

# counter to record how many new records are added
i = 0
# Search start with each NRIC,
list_disch_pt_nric = df_dc['Ext_Pat_No'].to_list()
# list_disch_pt_nric = ['S0484720B']  # test case
for item_NRIC in list_disch_pt_nric:
    df_dc_search = df_dc.loc[df_dc['Ext_Pat_No'] == item_NRIC, :]
    list_disch_date = df_dc_search['Disch_Date'].to_list()
    # within each NRIC check for each visit against the discharge database. if valid append into the master case
    # dataframe
    for disch_date in list_disch_date:
        check_case_no = df_dc_search[df_dc_search.Disch_Date == disch_date].Case_No.values[0]
        if not (check_case_no in df_master_case_list.Case_ID.astype(str).values):
            valid_disch = check_case_in_soc(item_NRIC, disch_date, df_soc)
            print('verify patient: ', item_NRIC, ' disch date:', disch_date, ' ', valid_disch)
            temp_list = []
            if valid_disch:
                df_disch_case = df_dc_search.loc[df_dc_search['Disch_Date'] == disch_date, :]
                # print(df_soc_search_case)
                case_ID = df_disch_case.iloc[0]['Case_No']
                temp_list.insert(0, case_ID)
                temp_list.insert(0, item_NRIC)
                temp_list.insert(0, disch_date)
                to_append = temp_list
                df_length = len(df_master_case_list)
                df_master_case_list.loc[df_length] = to_append
                i = i + 1
            else:
                pass
        else:
            print("Patient: ", item_NRIC, ' discharged on:', disch_date, " already in database")
            pass

if i > 0:
    df_master_case_list.to_csv(PT.path_wip_output + 'master_SOC_BSC_list_dc.csv', index=0)
    print(i, ' new records added to the master list')
else:
    print('No new records added to the master list')

exclusion_list_disch_type = ['Absconded', 'Death Coroner', 'Death NCoroner', 'Dis agst advice', 'Dis. Comm Hosp',
                             'Dis. NHG Hosp', 'Dis. Singhealth']

exclusion_list_ref = [' Khoo Teck Puat Hospital', ' Ministry of Health (HQ)', ' Sengkang Hospital',
                      ' Singapore General Hospital', ' Tan Tock Seng Hospital', ' Yishun Community Hospital']

df_master_case_list = pd.read_csv(PT.path_wip_output + 'master_SOC_BSC_list_dc.csv')
df_dc_denominator = df_dc[~df_dc['Discharge_Type_Text'].isin(exclusion_list_disch_type)]
df_dc_denominator = df_dc_denominator[~df_dc_denominator['Referring_Hospital_Text'].isin(exclusion_list_ref)]
df_dc_denominator = df_dc_denominator.loc[df_dc_denominator['Adm_Type'].str.contains('EM|SD|DI|EL|SO|TA', regex=True)]
df_dc_denominator['Year'] = df_dc_denominator.Disch_Date.dt.year
df_dc_denominator['Quarter'] = df_dc_denominator.Disch_Date.dt.quarter

df_master_case_list['Date'] = df_master_case_list.apply((lambda x: parse(x['Disch_Date'])), axis=1)
df_master_case_list['Year'] = df_master_case_list.Date.dt.year
df_master_case_list['Quarter'] = df_master_case_list.Date.dt.quarter

df_report_numerator = pd.pivot_table(df_master_case_list, values='Case_ID', index=['Year', 'Quarter'], aggfunc='count')
df_report_denominator = pd.pivot_table(df_dc_denominator, values='Case_No', index=['Year', 'Quarter'], aggfunc='count')
df_report_denominator.rename(columns={"Case_No": 'Case_ID'}, inplace=True)

writer = pd.ExcelWriter(PT.path_report_output + 'BSC_SO_1.2.xlsx', engine='xlsxwriter')
df_report_BSC = df_report_numerator/df_report_denominator
df_report_BSC.rename(columns={"Case_ID": 'SO1.2'}, inplace=True)
df_report_BSC.to_excel(writer, sheet_name='BSC_SO1.2_tracking')
df_report_numerator.to_excel(writer, sheet_name='numerator')
df_report_denominator.to_excel(writer, sheet_name='denominator')
df_master_case_list.to_excel(writer, sheet_name='numerator_raw', index=0)
df_dc_denominator.to_excel(writer, sheet_name='denominator_raw', index=0)

writer.save()
