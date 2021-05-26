import pandas as pd
import path as PT
import datetime

desired_width = 6200
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 12)


# check whether the particular visit is within 180 days of qualified post discharge period
# need to extract the closet valid discharge date based on SOC visit date, and check for the difference
def check_case_in_disch(pt_nric, date_of_SOC_visit, df_disch):
    df_dc_search = df_disch.loc[df_dc['Ext_Pat_No'] == pt_nric, :]
    list_of_disch_date = df_dc_search['Disch_Date'].to_list()
    diff_func = lambda list_value: (date_of_SOC_visit - list_value)
    try:
        closest_value = min(list_of_disch_date, key=diff_func)
        delta_duration = date_of_SOC_visit - closest_value
        if delta_duration < datetime.timedelta(days=180):
            return True
        else:
            return False
    except:
        pass
    print('verify patient: ', pt_nric, ' visit date:', date_of_SOC_visit, ' done')


# Load the data source with necessary filters
df_dc = pd.read_parquet(PT.path_wip_output + 'Combined_discharge.parquet',
                        columns=['Case_No', 'Ext_Pat_No', 'Dept_OU', 'Adm_Date', 'Disch_Date', 'Adm_Type',
                                 'Discharge_Physician_Name', 'Pri_Diag_Code_Text']).sort_values(by=['Disch_Date'])
df_dc = df_dc.loc[df_dc['Dept_OU'].str.contains('LSHAGERI|LSCHRO|LSFAMED', regex=True)]

df_soc = pd.read_parquet(PT.path_wip_output + 'Combined_SOC.parquet',
                         columns=['Case_No', 'Ext_Pat_ID', 'Sub-Specialty_ID', 'Visit_Date', 'Visit_Type',
                                  'Referral_Hospital', 'Attn_Phy', 'Comments']).sort_values(by=['Visit_Date'])

df_soc = df_soc.loc[df_soc['Visit_Type'].str.contains('FV|RV', regex=True)]
df_soc = df_soc.loc[df_soc['Sub-Specialty_ID'].str.contains('LSHAGERI|LSCHRO', regex=True)]
df_soc = df_soc.loc[df_soc['Referral_Hospital'].str.contains('Intra-Dept referral Ward')]
df_soc = df_soc.loc[
    df_soc['Attn_Phy'].str.contains('APN|PODIATRIST|PHYSIO|PHARMACIST|PSYCHOLOGIST', regex=True) == False]

# initiate the output dataframe. If the data files is not found, initiate a new empty data frame
try:
    df_master_case_list = pd.read_csv(PT.path_wip_output + 'master_SOC_BSC_list.csv', index_col='Case_ID').reset_index()
    print(df_master_case_list)
except:
    df_master_case_list = pd.DataFrame(columns=["Visit_Date", "Ext_Pat_ID", "Case_ID"])

# counter to record how many new records are added
i = 0
# Search start with each NRIC,
list_SOC_pt_nric = df_soc['Ext_Pat_ID'].to_list()
for item_NRIC in list_SOC_pt_nric:
    df_soc_search = df_soc.loc[df_soc['Ext_Pat_ID'] == item_NRIC, :]
    list_soc_visit_date = df_soc_search['Visit_Date'].to_list()
    # within each NRIC check for each visit against the discharge database. if valid append into the master case
    # dataframe
    for v_date in list_soc_visit_date:
        check_case_no = df_soc_search[df_soc_search.Visit_Date == v_date].Case_No.values[0]
        if not (check_case_no in df_master_case_list.Case_ID.astype(str).values):
            valid_soc_visit = check_case_in_disch(item_NRIC, v_date, df_dc)
            temp_list = []
            if valid_soc_visit:
                df_soc_search_case = df_soc_search.loc[df_soc_search['Visit_Date'] == v_date, :]
                # print(df_soc_search_case)
                case_ID = df_soc_search_case.iloc[0]['Case_No']
                temp_list.insert(0, item_NRIC)
                temp_list.insert(0, v_date)
                temp_list.insert(0, case_ID)
                to_append = temp_list
                df_length = len(df_master_case_list)
                df_master_case_list.loc[df_length] = to_append
                i = i + 1
            else:
                pass
        else:
            pass

if i > 0:
    df_master_case_list.to_csv(PT.path_wip_output + 'master_SOC_BSC_list.csv', index=0)
    print(i, ' new records added to the master list')
else:
    print('No new records added to the master list')

