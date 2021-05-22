import os
import pandas as pd
import numpy as np


def clean_raw_extraction(source_path, filename, wip_path, lookup_path):
    # inspect the first 50 rows to find out which row start with actual data header.
    # Assume all report will have case number field
    # idx is the index row for the actual data starts, use this value to skip header when do 2nd import
    df = pd.read_table(source_path + filename, sep='/t',
                       skip_blank_lines=False, nrows=50,
                       engine='python')
    col = df.columns
    idx = df[df[col[0]].astype(str).str.contains("Case")].index.values

    # read raw download file with 'skiprows' based on idx value obtained

    df = pd.read_csv(source_path + filename, sep='\t',
                     skiprows=idx[0], skipfooter=1, skip_blank_lines=True,
                     engine='python')
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
    # Assume all reports will have 'Case No' field, use it to filter for AlEX cases (2800)
    df = df.loc[df['Case_No'].notna(), :]
    df = df.loc[df['Case_No'].str.contains('2800'), :]
    df['cnt'] = 1
    df = Date_Conversion(df)
    mapping_Trt_Cat(df, lookup_path)
    print(filename, " Processed")
    # df.to_csv(wip_path + filename + '.csv', index=False)
    return df


def Date_Conversion(df_to_convert):
    headers = [header for header in df_to_convert.columns]
    if any('Adm_Date' in string for string in headers):
        df_to_convert['Adm_Date'] = pd.to_datetime(df_to_convert['Adm_Date'], format='%d.%m.%Y')
    if any('Admit_Date' in string for string in headers):
        df_to_convert['Admit_Date'] = pd.to_datetime(df_to_convert['Admit_Date'], format='%d.%m.%Y')
    if any('Disch_Date' in string for string in headers):
        df_to_convert['Disch_Date'] = pd.to_datetime(df_to_convert['Disch_Date'], format='%d.%m.%Y')
    if any('Inflight_Date' in string for string in headers):
        df_to_convert['Inflight_Date'] = pd.to_datetime(df_to_convert['Inflight_Date'], format='%d.%m.%Y')
    if any('Operation_Date' in string for string in headers):
        df_to_convert['Operation_Date'] = pd.to_datetime(df_to_convert['Operation_Date'], format='%d.%m.%Y')
    if any('Visit_Date' in string for string in headers):
        df_to_convert['Visit_Date'] = pd.to_datetime(df_to_convert['Visit_Date'], format='%d.%m.%Y')
    if any('Visit_Date' in string for string in headers):
        df_to_convert['Visit_Date'] = pd.to_datetime(df_to_convert['Visit_Date'], format='%d.%m.%Y')
    if any('Trauma_Start_Date' in string for string in headers):
        df_to_convert['Trauma_Start_Date'] = pd.to_datetime(df_to_convert['Trauma_Start_Date'], format='%d.%m.%Y')
    if any('Trauma_End_Date' in string for string in headers):
        df_to_convert['Trauma_End_Date'] = pd.to_datetime(df_to_convert['Trauma_End_Date'], format='%d.%m.%Y')
    if any('PACS Start Date' in string for string in headers):
        df_to_convert['PACS_Start_Date'] = pd.to_datetime(df_to_convert['PACS_Start_Date'], format='%d.%m.%Y')
    if any('PACS_End_Date' in string for string in headers):
        df_to_convert['PACS_End_Date'] = pd.to_datetime(df_to_convert['PACS_End_Date'], format='%d.%m.%Y')
    if any('Discharge_Acuity_Level' in string for string in headers):
        df_to_convert['Trt_Cat'] = df_to_convert['Discharge_Acuity_Level']
    if any('Ref_Type' in string for string in headers):
        df_to_convert['Referral_type'] = df_to_convert['Ref_Type']
        df_to_convert = df_to_convert.drop(columns=['Ref_Type'])
    if any('Ref_Hosp 1' in string for string in headers):
        df_to_convert['Referral_Hospital'] = df_to_convert['Ref_Hosp_1']
        df_to_convert = df_to_convert.drop(columns=['Ref_Hosp 1'])
    if any('Ref_Hospital' in string for string in headers):
        df_to_convert['Referral_Hospital'] = df_to_convert['Ref_Hospital']
        df_to_convert = df_to_convert.drop(columns=['Ref_Hospital'])
    elif any('Ref_Hosp' in string for string in headers):
        df_to_convert['Referral_Hospital'] = df_to_convert['Ref_Hosp']
        df_to_convert = df_to_convert.drop(columns=['Ref_Hosp'])
    return df_to_convert


def mapping_Trt_Cat(df_to_merge, path_lookup):
    headers = [header for header in df_to_merge.columns]
    if any('Trt_Cat' == string for string in headers):
        df_acuity = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name='Acuity')
        df_to_merge = pd.merge(df_to_merge, df_acuity, how='left', on='Trt_Cat')

    if any('Class' == string for string in headers):
        df_pt_cls_abc = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name="pt_class_abc")
        # df_pt_resident = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name="Resident")
        df_to_merge = pd.merge(df_to_merge, df_pt_cls_abc, how='left', on='Class')
        # df_to_merge = pd.merge(df_to_merge, df_pt_resident, how='left', on='Class')

    if any('Dept_OU' == string for string in headers):
        df_pt_cls_abc = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name="Subspec")
        df_to_merge = pd.merge(df_to_merge, df_pt_cls_abc, how='left', on='Dept_OU')
        df_dept_MOH = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name="MOH_mapping")
        df_to_merge = pd.merge(df_to_merge, df_dept_MOH, how='left', on='Dept_OU')
        df_dept_prog = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name="Program")
        df_to_merge = pd.merge(df_to_merge, df_dept_prog, how='left', on='Dept_OU')
    if any('Adm_Dept_OU' == string for string in headers):
        df_dept_MOH_adm = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name="MOH_mapping_adm")
        df_to_merge = pd.merge(df_to_merge, df_dept_MOH_adm, how='left', on='Adm_Dept_OU')
    if any('Disch_Dept_OU' == string for string in headers):
        df_dept_prog_disch = pd.read_excel(path_lookup + 'Class.xlsx', sheet_name="MOH_mapping_disch")
        df_to_merge = pd.merge(df_to_merge, df_dept_prog_disch, how='left', on='Disch_Dept_OU')
    return df_to_merge


def pt_class_with_icu_iso(pt_class, acc_cat, trt_cat):
    if acc_cat == 'ISO':
        pt_class = 'ISO'
    elif trt_cat[0:3] == 'CCU':
        pt_class = 'ICU'
    elif trt_cat[0:2] == 'HD':
        pt_class = 'HD'
    else:
        pt_class = pt_class
    return pt_class


def pt_cls_icu_iso_for_disch(pt_class, iso_ward, trt_cat):
    if iso_ward[0:3] == 'LW9':
        pt_class = 'ISO'
    elif iso_ward[0:3] == 'LW8':
        pt_class = 'ISO'
    elif trt_cat[0:2] == 'HD':
        pt_class = 'HD'
    elif trt_cat[0:3] == 'CCU':
        pt_class = 'ICU'
    else:
        pt_class = pt_class
    return pt_class


def replace_with_current_ward(adm_ward, current_ward):
    left_str = adm_ward[0:2]
    if left_str == 'LW':
        if adm_ward == 'LWEDTU':
            adm_ward = current_ward
            return adm_ward
        if adm_ward == 'LWASW':
            adm_ward = current_ward
            return adm_ward
        if adm_ward == 'LWDSW':
            adm_ward = current_ward
            return adm_ward
    else:
        adm_ward = current_ward
    return adm_ward


def combine_EM_EL(Adm_type):
    Adm_type = Adm_type[0:2]
    if Adm_type == 'EM':
        Adm_type = "Emergency"
        return Adm_type
    if Adm_type == 'DI':
        Adm_type = 'Emergency'
        return Adm_type
    if Adm_type == 'EL':
        Adm_type = 'Elective'
        return Adm_type
    if Adm_type == 'SD':
        Adm_type = 'Elective'
        return Adm_type
    else:
        Adm_type = 'Other Adm Types'
    return Adm_type


def death_indicator(discharge_Type_text):
    if discharge_Type_text[0:5] == 'Death':
        return 1
    else:
        return 0


def fin_ref_hosp_inpt(refer_hosp):
    if 'Intra-Dept referral A&E' in refer_hosp:
        return 'Intra-A&E'
    elif 'Intra-Dept referral SOC' in refer_hosp:
        return 'Intra-SOC'
    elif 'National University Hospital' in refer_hosp:
        return 'NUH'
    elif 'Hospital' in refer_hosp:
        return 'Other inter-Hosp trf'
    else:
        return 'Others'


def fin_ref_hosp_soc(ref_type, ref_hosp):
    if 'Polyclinics' in ref_type:
        return 'Polyclinics'
    elif 'Poly' in ref_type:
        return 'Polyclinics'
    elif 'National University Hospital' in ref_hosp:
        return 'NUH'
    elif 'GP' in ref_type:
        return 'GP'
    elif 'CommHlth Assist' in ref_type:
        return 'GP'
    elif 'Pte Practitione' in ref_type:
        return 'GP'
    else:
        return 'Others'


def prelim_flag_enter_validation():
    question = "Would you like extract this file as prelim? (y/Y or n/N): "
    choice = input(question)
    while choice not in ['y', 'Y', 'n', 'N']:
        print('Invalid choice')
        choice = input(question)
    return choice
