import os
import pandas as pd
import numpy as np
import data_prep
import data_prep as prep  # custom module for file cleaning
import path as PT

# to indicate whether want to save output as csv, along with the default parquet output.
# no CSV output while equal to 0.
require_csv = 0
# import locale
# locale.setlocale(locale.LC_TIME, "en_SG")  # singapore
desired_width = 6200
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 12)
# Step 1 -  combine inflight data
# # # scan file log
# filename_db = 'Combined_inflight.parquet.gzip'
# filename_csv = 'Combined_inflight.csv'
# filename_log = 'inflight_file_log.xlsx'
# df_file_log = pd.read_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")
# list_file_log = df_file_log['File'].tolist()
#
# # load the existing database, drop prelim data. if DB not found, reset to empty DB, and reset the filelist to empty
# try:
#     df_inflight = pd.read_parquet(PT.path_wip_output + filename_db)
#     df_inflight = df_inflight.loc[df_inflight['prelim_flag'] != 'Y']
#     df_inflight.drop_duplicates(inplace=True)
# except:
#     df_inflight = pd.DataFrame()
#     list_file_log = []
# # scan the files in the folder and compare with file log, to extract delta file list for extraction
# list_all_files = [file for file in os.listdir(PT.path_inflight_data) if file.__contains__('.zip')]
# files = [item for item in list_all_files if item not in list_file_log]
# print("Inflight files to merge: ", files, end='\n')
#
# if len(files) != 0:
#     # extraction for delta files only. Only update the filelist if prelim_flag is N/n
#     for file in files:
#         print(file, end='\n')
#         prelim_flag = data_prep.prelim_flag_enter_validation()
#         if prelim_flag.upper() != 'Y':
#             list_file_log.append(file)
#             # print(list_file_log,end='\n')
#         current_data = prep.clean_raw_extraction_zip(PT.path_inflight_data, file, PT.path_wip_output, PT.path_lookup)
#         current_data['prelim_flag'] = prelim_flag.upper()
#         df_inflight = pd.concat([df_inflight, current_data])
#
#     # convert file list to df and write back to the log file
#     df_file_log = pd.DataFrame(list_file_log, columns=['File'])
#     df_file_log.to_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")
#
#     if require_csv != 0:
#         df_inflight.to_csv(PT.path_wip_output + filename_csv, index=False)
#     df_inflight.to_parquet(PT.path_wip_output + filename_db, compression='gzip', index=False)
# else:
#     pass
# print("Inflight data: ", df_inflight.shape, 'Current data include below extraction files as final:')
# print(df_file_log)
# print("Step 1 successfully completed - Inflight data done")
#
# # Step 2 -  combine discharge data
# # scan file log
#
# filename_db = 'Combined_disch.parquet.gzip'
# filename_csv = 'Combined_disch.csv'
# filename_log = 'disch_file_log.xlsx'
# df_file_log = pd.read_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")
# list_file_log = df_file_log['File'].tolist()
# # load the existing database, drop prelim data. if DB not found, reset to empty DB, and reset the filelist to empty
# try:
#     df_dc = pd.read_parquet(PT.path_wip_output + filename_db)
#     df_dc = df_dc.loc[df_dc['prelim_flag'] != 'Y']
#     df_dc.drop_duplicates(inplace=True)
# except:
#     df_dc = pd.DataFrame()
#     list_file_log = []
# # scan the files in the folder and compare with file log, to extract delta file list for extraction
# list_all_files = [file for file in os.listdir(PT.path_discharge_data) if file.__contains__('.zip')]
# files = [item for item in list_all_files if item not in list_file_log]
# print("Discharge data files to merge: ", files, end='\n')
#
# if len(files) != 0:
#     # extraction for delta files only. Only update the filelist if prelim_flag is N/n
#     for file in files:
#         print(file, end='\n')
#         prelim_flag = data_prep.prelim_flag_enter_validation()
#         if prelim_flag.upper() != 'Y':
#             list_file_log.append(file)
#             # print(list_file_log)
#         current_data = prep.clean_raw_extraction_zip(PT.path_discharge_data, file, PT.path_wip_output, PT.path_lookup)
#         current_data['prelim_flag'] = prelim_flag.upper()
#         df_dc = pd.concat([df_dc, current_data])
#
#     # convert file list to df and write back to the log file
#     df_file_log = pd.DataFrame(list_file_log, columns=['File'])
#     df_file_log.to_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")
#
#     # to avoid the encoding error
#     df_dc['Postal'] = df_dc['Postal'].str.slice(0, 6)
#     df_dc['Disch_Type'] = df_dc['Disch_Type'].str.slice(0, 2)
#     if require_csv != 0:
#         df_dc.to_csv(PT.path_wip_output + filename_csv, index=False)
#     df_dc.to_parquet(PT.path_wip_output + filename_db, compression='gzip', index=False)
# else:
#     pass
# print("Discharge data: ", df_dc.shape, 'Current data include below extraction files as final:')
# print(df_file_log)
# print("Step 2 successfully completed - discharge data done")

# Step 3 -  combine admission data
# scan file log
filename_db = 'Combined_adm.parquet.gzip'
filename_csv = 'Combined_adm.csv'
filename_log = 'adm_file_log.xlsx'
df_file_log = pd.read_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")
list_file_log = df_file_log['File'].tolist()
# load the existing database, drop prelim data. if DB not found, reset to empty DB, and reset the filelist to empty
try:
    df_adm = pd.read_parquet(PT.path_wip_output + filename_db)
    df_adm = df_adm.loc[df_adm['prelim_flag'] != 'Y']
    df_adm.drop_duplicates(inplace=True)
except:
    df_adm = pd.DataFrame()
    list_file_log = []
# scan the files in the folder and compare with file log, to extract delta file list for extraction
list_all_files = [file for file in os.listdir(PT.path_admission_data) if file.__contains__('.zip')]
files = [item for item in list_all_files if item not in list_file_log]
print("Admission data files to merge: ", files, end='\n')

if len(files) != 0:
    # extraction for delta files only. Only update the filelist if prelim_flag is N/n
    for file in files:
        print(file, end='\n')
        prelim_flag = data_prep.prelim_flag_enter_validation()
        if prelim_flag.upper() != 'Y':
            list_file_log.append(file)
            # print(list_file_log)
        current_data = prep.clean_raw_extraction_zip(PT.path_admission_data, file, PT.path_wip_output, PT.path_lookup)
        current_data['prelim_flag'] = prelim_flag.upper()
        df_adm = pd.concat([df_adm, current_data])

    # convert file list to df and write back to the log file
    df_file_log = pd.DataFrame(list_file_log, columns=['File'])
    df_file_log.to_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")

    # to avoid the encoding error
    df_adm['Postal_Code'] = df_adm['Postal_Code'].str.slice(0, 6)
    if require_csv != 0:
        df_adm.to_csv(PT.path_wip_output + filename_csv, index=False)
    df_adm.to_parquet(PT.path_wip_output + filename_db, compression='gzip', index=False)
else:
    pass
print("admission data: ", df_adm.shape, 'Current data include below extraction files as final:')
print(df_file_log)
print("Step 3 successfully completed - admission data done")

# Step 4 - combine procedure data
# scan file log
filename_db = 'Combined_procedure.parquet.gzip'
filename_csv = 'Combined_procedure.csv'
filename_log = 'procedure_file_log.xlsx'
df_file_log = pd.read_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")
list_file_log = df_file_log['File'].tolist()
# load the existing database, drop prelim data. if DB not found, reset to empty DB, and reset the filelist to empty
try:
    df_procedure = pd.read_parquet(PT.path_wip_output + filename_db)
    df_procedure = df_procedure.loc[df_procedure['prelim_flag'] != 'Y']
    df_procedure.drop_duplicates(inplace=True)
except:
    df_procedure = pd.DataFrame()
    list_file_log = []
# scan the files in the folder and compare with file log, to extract delta file list for extraction
list_all_files = [file for file in os.listdir(PT.path_procedure_data) if file.__contains__('.zip')]
files = [item for item in list_all_files if item not in list_file_log]
print("Procedure data files to merge: ", files, end='\n')

if len(files) != 0:
    # extraction for delta files only. Only update the filelist if prelim_flag is N/n
    for file in files:
        print(file, end='\n')
        prelim_flag = data_prep.prelim_flag_enter_validation()
        if prelim_flag.upper() != 'Y':
            list_file_log.append(file)
            # print(list_file_log)
        current_data = prep.clean_raw_extraction_zip(PT.path_procedure_data, file, PT.path_wip_output, PT.path_lookup)
        current_data['prelim_flag'] = prelim_flag.upper()
        df_procedure = pd.concat([df_procedure, current_data])

    # convert file list to df and write back to the log file
    df_file_log = pd.DataFrame(list_file_log, columns=['File'])
    df_file_log.to_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")
    if require_csv != 0:
        df_procedure.to_csv(PT.path_wip_output + filename_csv, index=False)
    df_procedure.to_parquet(PT.path_wip_output + filename_db, compression='gzip', index=False)
else:
    pass
print("procedure data: ", df_procedure.shape, 'Current data include below extraction files as final:')
print(df_file_log)
print("Step 4 successfully completed - procedure data done")

# # Step 5 - combine UCC data
# scan file log
filename_db = 'Combined_UCC.parquet.gzip'
filename_csv = 'Combined_UCC.csv'
filename_log = 'ucc_file_log.xlsx'
df_file_log = pd.read_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")
list_file_log = df_file_log['File'].tolist()
# load the existing database, drop prelim data. if DB not found, reset to empty DB, and reset the filelist to empty
try:
    df_UCC = pd.read_parquet(PT.path_wip_output + filename_db)
    df_UCC = df_UCC.loc[df_UCC['prelim_flag'] != 'Y']
    df_UCC.drop_duplicates(inplace=True)
except:
    df_UCC = pd.DataFrame()
    list_file_log = []
# scan the files in the folder and compare with file log, to extract delta file list for extraction
list_all_files = [file for file in os.listdir(PT.path_UCC_data) if file.__contains__('.zip')]
files = [item for item in list_all_files if item not in list_file_log]
print("UCC data files to merge: ", files, end='\n')

if len(files) != 0:
    # extraction for delta files only. Only update the filelist if prelim_flag is N/n
    for file in files:
        print(file, end='\n')
        prelim_flag = data_prep.prelim_flag_enter_validation()
        if prelim_flag.upper() != 'Y':
            list_file_log.append(file)
            # print(list_file_log)
        current_data = prep.clean_raw_extraction_zip(PT.path_UCC_data, file, PT.path_wip_output, PT.path_lookup)
        current_data['prelim_flag'] = prelim_flag.upper()
        df_UCC = pd.concat([df_UCC, current_data])

    # convert file list to df and write back to the log file
    df_file_log = pd.DataFrame(list_file_log, columns=['File'])
    df_file_log.to_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")

    # to avoid the encoding error
    df_UCC['Case_End_Type_Code'] = df_UCC['Case_End_Type_Code'].str.slice(0, 2)
    if require_csv != 0:
        df_UCC.to_csv(PT.path_wip_output + filename_csv, index=False)
    df_UCC.to_parquet(PT.path_wip_output + filename_db, compression='gzip', index=False)
else:
    pass
print("UCC data: ", df_UCC.shape, 'Current data include below extraction files as final:')
print(df_file_log)
print("Step 5 successfully completed - UCC data done")

# # Step 6 - combine SOC datan
# scan file log
filename_db = 'Combined_SOC.parquet.gzip'
filename_csv = 'Combined_SOC.csv'
filename_log = 'soc_file_log.xlsx'
df_file_log = pd.read_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")
list_file_log = df_file_log['File'].tolist()

# load the existing database, drop prelim data. if DB not found, reset to empty DB, and reset the filelist to empty
try:
    df_SOC = pd.read_parquet(PT.path_wip_output + filename_db)
    df_SOC = df_SOC.loc[df_SOC['prelim_flag'] != 'Y']
    df_SOC.drop_duplicates(inplace=True)
except:
    df_SOC = pd.DataFrame()
    list_file_log = []
# scan the files in the folder and compare with file log, to extract delta file list for extraction
list_all_files = [file for file in os.listdir(PT.path_SOC_data) if file.__contains__('.zip')]
files = [item for item in list_all_files if item not in list_file_log]
print("SOC data files to merge: ", files, end='\n')

if len(files) != 0:
    # extraction for delta files only. Only update the filelist if prelim_flag is N/n
    for file in files:
        print(file, end='\n')
        prelim_flag = data_prep.prelim_flag_enter_validation()
        if prelim_flag.upper() != 'Y':
            list_file_log.append(file)
            print(list_file_log)
        current_data = prep.clean_raw_extraction_zip(PT.path_SOC_data, file, PT.path_wip_output, PT.path_lookup)
        current_data['prelim_flag'] = prelim_flag.upper()
        df_SOC = pd.concat([df_SOC, current_data])

    # convert file list to df and write back to the log file
    df_file_log = pd.DataFrame(list_file_log, columns=['File'])
    df_file_log.to_excel(PT.path_wip_output_log + filename_log, sheet_name="file_log")

    # to avoid the encoding error
    df_SOC['Postal_Code'] = df_SOC['Postal_Code'].str.slice(0, 6)
    if require_csv != 0:
        df_SOC.to_csv(PT.path_wip_output + filename_csv, index=False)
    df_SOC.to_parquet(PT.path_wip_output + filename_db, compression='gzip', index=False)
else:
    pass
print("SOC data: ", df_SOC.shape, 'Current data include below extraction files as final:')
print(df_file_log)
print("Step 6 successfully completed - SOC data done")

#######################################################################
