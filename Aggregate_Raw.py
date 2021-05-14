import os
import pandas as pd
import numpy as np
import data_prep as prep              # custom module for file cleaning
import path as PT
# import locale
# locale.setlocale(locale.LC_TIME, "en_SG")  # singapore

desired_width = 6200
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 12)

# Step 1
# combine inflight data
files = [file for file in os.listdir(PT.path_inflight_data) if file.__contains__('.XLS')]
print("Inflight files to merge: ", files)

df_inflight = pd.DataFrame()
for file in files:
    current_data = prep.clean_raw_extraction(PT.path_inflight_data, file, PT.path_wip_output, PT.path_lookup)
    df_inflight = pd.concat([df_inflight, current_data])

df_inflight.to_csv(PT.path_wip_output+"Combined_inflight.csv", index=False)
df_inflight.to_parquet(PT.path_wip_output+"Combined_inflight.parquet", index=False)
print("Inflight data: ", df_inflight.shape)
print("Step 1 successfully completed - Inflight data done")

# Step 2
# combine Discharge data
files = [file for file in os.listdir(PT.path_discharge_data) if file.__contains__('.XLS')]
print("discharge files to merge: ", files)

df_dc = pd.DataFrame()
for file in files:
    current_data = prep.clean_raw_extraction(PT.path_discharge_data, file, PT.path_wip_output, PT.path_lookup)
    df_dc = pd.concat([df_dc, current_data])

# to avoid the encoding error
df_dc['Postal'] = df_dc['Postal'].str.slice(0, 6)
df_dc['Disch_Type'] = df_dc['Disch_Type'].str.slice(0, 2)

df_dc.to_csv(PT.path_wip_output+'Combined_discharge.csv', index=False)
df_dc.to_parquet(PT.path_wip_output+"Combined_discharge.parquet", index=False)
print("Discharge data: ", df_dc.shape)
print("Step 2 successfully completed - discharge data done")

# Step 3
# combine admission data
files = [file for file in os.listdir(PT.path_admission_data) if file.__contains__('.XLS')]
print("admission files to merge: ", files)

df_adm = pd.DataFrame()
for file in files:
    current_data = prep.clean_raw_extraction(PT.path_admission_data, file, PT.path_wip_output, PT.path_lookup)
    df_adm = pd.concat([df_adm, current_data])

# to avoid the encoding error
df_adm['Postal_Code'] = df_adm['Postal_Code'].str.slice(0, 6)

df_adm.to_csv(PT.path_wip_output + 'Combined_admission.csv', index=False)
df_adm.to_parquet(PT.path_wip_output+"Combined_admission.parquet", index=False)

print("Adm data: ", df_adm.shape)
print("Step 3 successfully completed - admission data done")


# Step 4
# combine procedure data
files = [file for file in os.listdir(PT.path_procedure_data) if file.__contains__('.XLS')]
print("procedure files to merge: ", files)

df_procedure = pd.DataFrame()
for file in files:
    current_data = prep.clean_raw_extraction(PT.path_procedure_data, file, PT.path_wip_output, PT.path_lookup)
    df_procedure = pd.concat([df_procedure, current_data])

df_procedure.to_csv(PT.path_wip_output + 'Combined_procedure.csv', index=False)
df_procedure.to_parquet(PT.path_wip_output+"Combined_procedure.parquet", index=False)
print("Procedure data: ", df_procedure.shape)
print("Step 4 successfully completed - procedure data done")


# Step 5
# combine UCC data
files = [file for file in os.listdir(PT.path_UCC_data) if file.__contains__('.XLS')]
print("UCC files to merge: ", files)

df_UCC = pd.DataFrame()
for file in files:
    current_data = prep.clean_raw_extraction(PT.path_UCC_data, file, PT.path_wip_output, PT.path_lookup)
    df_UCC = pd.concat([df_UCC, current_data])

# to avoid the encoding error
df_UCC['Case_End_Type_Code'] = df_UCC['Case_End_Type_Code'].str.slice(0, 2)

df_UCC.to_csv(PT.path_wip_output + 'Combined_UCC.csv', index=False)
df_UCC.to_parquet(PT.path_wip_output+"Combined_UCC.parquet", index=False)
print("UCC data: ", df_UCC.shape)
print("Step 5 successfully completed - UCC data done")



# Step 6
# combine SOC data
files = [file for file in os.listdir(PT.path_SOC_data) if file.__contains__('.XLS')]
print("SOC files to merge: ", files)

df_SOC = pd.DataFrame()
for file in files:
    current_data = prep.clean_raw_extraction(PT.path_SOC_data, file, PT.path_wip_output, PT.path_lookup)
    df_SOC = pd.concat([df_SOC, current_data])

# to avoid the encoding error
df_SOC['Postal_Code'] = df_SOC['Postal_Code'].str.slice(0, 6)

df_SOC.to_csv(PT.path_wip_output + 'Combined_SOC.csv', index=False)
df_SOC.to_parquet(PT.path_wip_output+"Combined_SOC.parquet", index=False)
print("SOC data: ", df_SOC.shape)
print("Step 6 successfully completed - SOC data done")
#






















