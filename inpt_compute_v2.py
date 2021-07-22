import pandas as pd
import numpy as np
import path as PT
import calendar
from calendar import monthrange


class acuity_compute:

    #Generating Acuity reports
    def sum_pt_days(inflight_df):
        pt_days_sum = pd.pivot_table(inflight_df, values='cnt', index=['Acuity'],
                                     columns=['Year', 'Month'],
                                     aggfunc=np.sum, margins=True, margins_name='Total')
        return pt_days_sum

    def sum_pt_days_percent(pivot_ptdays_df):
        df_percent = pivot_ptdays_df.copy()
        for i in df_percent.columns:
            df_percent[i] = df_percent[i] / df_percent[i][len(
                df_percent) - 1]
            df_percent[i] = df_percent[i].astype(float).map("{:.1%}".format)
        return df_percent

    def ALOS(inflight_df, pivot_ptdays_df):
        df_cnt = pd.pivot_table(inflight_df, values='Case_No', index=['Acuity'],
                                columns=['Year', 'Month'],
                                aggfunc=pd.Series.nunique, margins=True, margins_name='Total')

        df_cnt.rename(columns={"Case_No": "cnt"}, level=0, inplace=True)

        df_ALOS = (pivot_ptdays_df / df_cnt).round(decimals=1)
        df_ALOS.drop(index='Total', axis=0, inplace=True)
        df_ALOS.drop(columns=('Total', ''), axis=1, inplace=True)
        # df_ALOS.droplevel(0)
        return df_ALOS

    def sum_pt_days_dept(inflight_df):
        pt_days_sum = pd.pivot_table(inflight_df, values='cnt', index=['Acuity', 'Dept_Name'],
                                     columns=['Year', 'Month'],
                                     aggfunc=np.sum, margins=True, margins_name='Total')

        pt_days_sum_sub_total = pt_days_sum.groupby(level='Acuity').sum()
        pt_days_sum_sub_total.index = [pt_days_sum_sub_total.index, ['Σ Sub-total'] * len(pt_days_sum_sub_total)]
        final_df_ptdays_sum = pd.concat([pt_days_sum, pt_days_sum_sub_total]).sort_index()
        final_df_ptdays_sum.drop(index=('Total', 'Σ Sub-total'), axis=0, inplace=True)
        final_df_ptdays_sum = final_df_ptdays_sum.fillna(0).astype(int)
        final_df_ptdays_sum.rename_axis(index={"Dept_Name": "Department"}, inplace=True)
        return final_df_ptdays_sum

    def sum_pt_days_dept_percent(pivot_ptdays_dept_df):
        df_percent = pivot_ptdays_dept_df.copy()
        for i in df_percent.columns:
            df_percent[i] = df_percent[i] / df_percent[i][len(
                df_percent) - 1]
            df_percent[i] = df_percent[i].astype(float).map("{:.1%}".format)
        df_percent.rename_axis(index={"Dept_Name": "Department"}, inplace=True)
        return df_percent

    def ALOS_dept(inflight_df, pivot_ptdays_dept_df):
        df_cnt = pd.pivot_table(inflight_df, values='Case_No', index=['Acuity', 'Dept_Name'],
                                columns=['Year', 'Month'],
                                aggfunc=pd.Series.nunique, margins=True, margins_name='Total')

        df_cnt_subtotal = df_cnt.groupby(level='Acuity').sum()
        df_cnt_subtotal.index = [df_cnt_subtotal.index, ['Σ Sub-total'] * len(df_cnt_subtotal)]
        final_df_cnt = pd.concat([df_cnt, df_cnt_subtotal]).sort_index()
        final_df_cnt.drop(index=('Total', 'Σ Sub-total'), axis=0, inplace=True)
        final_df_cnt = final_df_cnt.fillna(0).astype(int)
        # final_df_cnt.rename(columns={"Case_No": "cnt"}, level=0, inplace=True)
        df_ALOS = (pivot_ptdays_dept_df / final_df_cnt).round(decimals=1)
        df_ALOS.drop(index=('Total', ''), axis=0, inplace=True)
        df_ALOS.drop(columns=('Total', ''), axis=1, inplace=True)
        df_ALOS.droplevel(0)
        df_ALOS.rename_axis(index={"Dept_Name": "Department"}, inplace=True)
        return df_ALOS



class bis_compute:

    # Generating BIS reports
    def bis_by_class(df_bis_for_report):
        report_df_bis_by_class = pd.pivot_table(df_bis_for_report, values='BIS', index=['Class'],
                                                columns=['Year', 'Month'],
                                                aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_bis_by_class.rename_axis(index={'Class': "Accommodation Class"}, inplace=True)
        return report_df_bis_by_class

    def bis_by_ward(df_bis_for_report):
        report_df_bis_by_ward = pd.pivot_table(df_bis_for_report, values='BIS', index=['Nrs_OU'],
                                               columns=['Year', 'Month'],
                                               aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_bis_by_ward.rename_axis(index={'Nrs_OU': "Ward"}, inplace=True)
        return report_df_bis_by_ward

    def avg_bis_class(df_bis_for_report):
        report_df_bis_by_class_avg = pd.pivot_table(df_bis_for_report, values='BIS', index=['Class'],
                                                    columns=['Year', 'Month'],
                                                    aggfunc=np.sum).round(decimals=0)
        report_df_bis_by_class_avg.rename_axis(index={'Class': "Accommodation Class"}, inplace=True)
        for i in report_df_bis_by_class_avg.columns:
            report_df_bis_by_class_avg[i] = report_df_bis_by_class_avg[i] / monthrange(i[0], i[1])[1]
        report_df_bis_by_class_avg = np.round(report_df_bis_by_class_avg, 0)
        report_df_bis_by_class_avg.loc[len(report_df_bis_by_class_avg)] = report_df_bis_by_class_avg.apply(
            np.sum).to_list()
        report_df_bis_by_class_avg = report_df_bis_by_class_avg.rename(
            index={len(report_df_bis_by_class_avg) - 1: 'TOTAL'})
        return report_df_bis_by_class_avg

    def avg_bis_ward(df_bis_for_report):
        report_df_bis_by_ward_avg = pd.pivot_table(df_bis_for_report, values='BIS', index=['Nrs_OU'],
                                                   columns=['Year', 'Month'],
                                                   aggfunc=np.sum).round(decimals=0)
        report_df_bis_by_ward_avg.rename_axis(index={'Nrs_OU':"Ward"}, inplace=True)
        # Gives number of days in the month and divides each column by those days
        for i in report_df_bis_by_ward_avg.columns:
            report_df_bis_by_ward_avg[i] = report_df_bis_by_ward_avg[i] / monthrange(i[0], i[1])[1]

        report_df_bis_by_ward_avg = np.round(report_df_bis_by_ward_avg, 0)
        # adding sum rows at the end of the table
        report_df_bis_by_ward_avg.loc[len(report_df_bis_by_ward_avg)] = report_df_bis_by_ward_avg.apply(
            np.sum).to_list()
        report_df_bis_by_ward_avg = report_df_bis_by_ward_avg.rename(
            index={len(report_df_bis_by_ward_avg) - 1: 'TOTAL'})
        return report_df_bis_by_ward_avg

class bor_compute:

    #Generating BOR Reports
    def bor_by_ward(df_pt_days_by_ward, df_bis_by_ward):
        report_df_BOR_by_ward = df_pt_days_by_ward / df_bis_by_ward
        report_df_BOR_by_ward = report_df_BOR_by_ward.applymap(
            lambda x: "{:.1%}".format(x) if pd.notna(x) else '')
        report_df_BOR_by_ward.rename_axis(index={'Nrs_OU': 'Ward'}, inplace=True)
        return report_df_BOR_by_ward

    def bor_by_class(df_inflight_final, df_bis_by_class):
        df_inflight_interim = df_inflight_final.copy()
        # combine HD & ICU Classes in pt_days_by_class dataframe
        df_inflight_interim['Accom_Class_icu_hd'] = df_inflight_interim.apply(
            lambda x: ('ICU' if x['Accom_Category'] == 'HD' else (
                'Classless' if x['Accom_Category'] == 'OTHER' else x['Accom_Category'])), axis=1)

        df_pt_days_by_class = pd.pivot_table(df_inflight_interim, values='cnt',
                                                index=['Accom_Class_icu_hd'], columns=['Year', 'Month'],
                                                aggfunc=np.sum, margins=True, margins_name='Total')

        report_df_BOR_by_class = df_pt_days_by_class.copy()
        for label in df_bis_by_class.index.tolist():
            if label in report_df_BOR_by_class.index.tolist():
                for col in df_pt_days_by_class.columns:
                    if col in df_bis_by_class.columns:
                        report_df_BOR_by_class.loc[label, col] = df_pt_days_by_class.loc[label,col]/df_bis_by_class.loc[label,col]
                    else:
                        report_df_BOR_by_class.loc[label, col] = 0
            else:
                report_df_BOR_by_class.loc[label] = 0
        report_df_BOR_by_class=report_df_BOR_by_class.sort_index(axis=0, ascending=True)
        report_df_BOR_by_class.rename_axis(index={'Accom_Class_icu_hd': 'Accommodation Class'}, inplace=True)
        report_df_BOR_by_class = report_df_BOR_by_class.applymap(
            lambda x: "{:.1%}".format(x) if pd.notna(x) else '')
        #report_df_BOR_by_class.to_csv(PT.path_wip_output + 'temp_bor_by_class.csv', index=True)
        #df_pt_days_by_class.to_csv(PT.path_wip_output + 'temp_pt_by_class.csv', index=True)
        #df_bis_by_class.to_csv(PT.path_wip_output + 'temp_bis_by_class.csv', index=True)
        return report_df_BOR_by_class


class admission_compute:

    # Generating Admission reports
    # Section 1: 1-month rolling Admission reports
    def F09_adm(df_adm_period, df_moh_speciality):
        report_df_F09_adm = pd.pivot_table(df_adm_period,
                                           values='cnt', index=['Moh_Clinical_Dept'],
                                           columns=['Year', 'Month', 'Resident_Type', 'Class_with_icu_iso'],
                                           aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_F09_adm = pd.merge(report_df_F09_adm, df_moh_speciality, how='right', on='Moh_Clinical_Dept')
        report_df_F09_adm.rename_axis(index={'Moh_Clinical_Dept': 'MOH Clinical Department'},
                                      columns={'Resident_Type': "Resident Status",
                                               'Class_with_icu_iso': 'Patient Class'}, inplace=True)
        return report_df_F09_adm

    def df_lodger_adm(df_adm_lodger, df_moh_speciality):
        report_df_lodger_adm = pd.pivot_table(df_adm_lodger, values='cnt', index=['Moh_Clinical_Dept'],
                                              columns=['Year', 'Month', 'Adm_Type_MOH', 'Wish_Cls', 'Adm_Acmd_Cat'],
                                              aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_lodger_adm = pd.merge(report_df_lodger_adm, df_moh_speciality, how='right', on='Moh_Clinical_Dept')
        report_df_lodger_adm.rename_axis(index={'Moh_Clinical_Dept': "MOH Clinical Department"},
                                         columns={'Adm_Type_MOH': "Admit Type", 'Wish_Cls': 'Patient Class',
                                                  'Adm_Acmd_Cat': 'Accommodation Class'},inplace=True)
        return report_df_lodger_adm

    # Section 2: 12 months rolling Admission reports
    def adm_by_admit_type(df_adm):
        # filter for elective/emergency cases
        df_adm_el_em_report = df_adm.loc[df_adm['Adm_Type'].str.contains('EM|DI|EL|SD', regex=True)]
        # df_adm_el_em_report = df_adm_el_em_report.loc[df_adm_el_em_report['Adm_Date'] >= end_date]
        df_subspec = pd.read_excel(PT.path_lookup + 'Class.xlsx', sheet_name="Subspec")
        df_subspec.rename(columns={'Dept_OU': 'Adm_Dept_OU'}, inplace=True)
        df_adm_el_em_report = pd.merge(df_adm_el_em_report, df_subspec, how='left', on='Adm_Dept_OU')
        report_df_adm_by_type = pd.pivot_table(df_adm_el_em_report, values='cnt', index=['Dept_Name'],
                                               columns=['Year', 'Month', 'Adm_Type_MOH', 'Adm_Sub_Type'],
                                               aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_adm_by_type.rename_axis(index={'Dept_Name': 'Department'},
                                          columns={'Adm_Type_MOH': 'Admit Type',
                                                   'Adm_Sub_Type': 'Admit Sub-Type'}, inplace=True)
        return report_df_adm_by_type

    def adm_by_paying(df_adm):
        report_df_adm_by_paying = pd.pivot_table(df_adm, values='cnt', index=['Paying_Status'],
                                                 columns=['Year', 'Month'], aggfunc=np.sum,
                                                 margins=True, margins_name='Total')
        report_df_adm_by_paying.rename_axis(index = {'Paying_Status':'Paying Status'}, inplace=True)
        return report_df_adm_by_paying

    def adm_by_ward(df_adm):
        report_df_adm_by_ward = pd.pivot_table(df_adm, values='cnt', index=['Adm_Ward'],
                                               columns=['Year', 'Month'], aggfunc=np.sum,
                                               margins=True, margins_name='Total')
        report_df_adm_by_ward.rename_axis(index={'Adm_Ward':'Ward'}, inplace=True)
        return report_df_adm_by_ward


class discharge_compute:

    #Generating Discharge reports
    # Section 1: 1-month rolling Discharge reports
    def F09_disch(df_dc_period, df_moh_speciality):
        report_df_F09_disch = pd.pivot_table(df_dc_period, values='cnt', index=['Moh_Clinical_Dept'],
                                             columns=['Year', 'Month', 'Resident_Type', 'cls_icu_iso'],
                                             aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_F09_disch = pd.merge(report_df_F09_disch, df_moh_speciality, how='right', on='Moh_Clinical_Dept')
        report_df_F09_disch.rename_axis(index={'Moh_Clinical_Dept': 'MOH Clinical Department'},
                                        columns={'Resident_Type': "Resident Status",
                                                 'cls_icu_iso': 'Patient Class',}, inplace=True)
        return report_df_F09_disch

    def F09_death(df_dc_period, df_moh_speciality):
        report_df_F09_disch_death = pd.pivot_table(df_dc_period, values='death', index=['Moh_Clinical_Dept'],
                                                   columns=['Year', 'Month', 'Resident_Type'],
                                                   aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_F09_disch_death = pd.merge(report_df_F09_disch_death, df_moh_speciality, how='right',
                                             on='Moh_Clinical_Dept')
        report_df_F09_disch_death.rename_axis(index={'Moh_Clinical_Dept':'MOH Clinical Department'},
                                              columns={'Resident_Type': "Resident Status"}, inplace=True)
        return report_df_F09_disch_death

    def daily_disch(df_dc_period):
        unique_year_mth = df_dc_period.loc[:, ['Year', 'Month']]
        unique_year_mth = unique_year_mth.drop_duplicates().sort_values(by=['Year', 'Month'], ascending=False)

        all_reports_df_daily_disch = []
        for index, row in unique_year_mth.iterrows():
            df_dc_period_i = df_dc_period.loc[(df_dc_period['Year'] == row['Year']) &
                                              (df_dc_period['Month'] == row['Month'])]
            #df_dc_period_i['Disch_Date'] = pd.to_datetime(df_dc_period_i['Disch_Date'], errors='coerce')
            df_dc_period_i["Month"] = df_dc_period_i['Month'].apply(lambda x: calendar.month_abbr[x])
            df_dc_period_i["Date"] = df_dc_period_i['Disch_Date'].dt.day
            report_df_daily_disch_i = pd.pivot_table(df_dc_period_i, values='cnt', index=['Nrs_OU'],
                                                     columns=['Year', 'Month', 'Date'],
                                                     aggfunc=np.sum, margins=True, margins_name='Total')
            report_df_daily_disch_i.rename_axis(index= {'Nrs_OU':'Ward'}, inplace=True)
            all_reports_df_daily_disch.append(report_df_daily_disch_i)
        return all_reports_df_daily_disch

    # Section 2: 12-months rolling Discharge reports
    def disch_by_ward(df_dc):
        report_df_disch_by_ward = pd.pivot_table(df_dc, values='cnt', index=['Nrs_OU'],
                                                 columns=['Year', 'Month'],
                                                 aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_disch_by_ward.rename_axis(index={'Nrs_OU': 'Ward'}, inplace=True)
        return report_df_disch_by_ward

    def disch_exclude_24h_by_type(df_dc_excl_24):
        report_df_disch_type = pd.pivot_table(df_dc_excl_24, values='cnt', index=['Discharge_Type_Text'],
                                          columns=['Year', 'Month'],
                                          aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_disch_type.rename_axis(index={'Discharge_Type_Text': 'Discharge Type'}, inplace=True)
        return report_df_disch_type

    def disch_in_24h(df_dc):
        report_df_disch_w_24h = pd.pivot_table(df_dc, values='Discharge_w_in_24_hrs', index=['Nrs_OU'],
                                               columns=['Year', 'Month'],
                                               aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_disch_w_24h.rename_axis(index={'Nrs_OU': 'Ward'}, inplace=True)
        return report_df_disch_w_24h

    def fin_disch_class_abc(df_dc):
        report_df_fin_disch_abc = pd.pivot_table(df_dc, values='cnt',
                                                 index=['Program', 'Dept_Name', 'Class_abc'],
                                                 columns=['Year', 'Month'],
                                                 aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_fin_disch_abc.rename_axis(index={'Dept_Name': 'Department', 'Class_abc': "Patient Class"},
                                            inplace=True)
        return report_df_fin_disch_abc

    def fin_disch_resident(df_dc):
        report_df_fin_disch_resident = pd.pivot_table(df_dc, values='cnt',
                                                      index=['Dept_Name', 'Resident_Type', 'Class_abc'],
                                                      columns=['Year', 'Month'],
                                                      aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_fin_disch_resident.rename_axis(index={'Dept_Name':'Department', 'Resident_Type':'Resident Status',
                                                        'Class_abc':'Patient Class'}, inplace=True)
        return report_df_fin_disch_resident

    def fin_disch_w_iso_HD(df_dc):
        report_df_fin_disch_w_iso_HD = pd.pivot_table(df_dc, values='cnt',
                                                      index=['Program', 'Dept_Name', 'cls_icu_iso'],
                                                      columns=['Year', 'Month'],
                                                      aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_fin_disch_w_iso_HD.rename_axis(index={'Dept_Name':'Department',
                                                        'cls_icu_iso':'Class'},
                                                 inplace=True)
        return report_df_fin_disch_w_iso_HD

    def fin_disch_dept(df_dc):
        report_df_fin_disch_dept = pd.pivot_table(df_dc, values='cnt',
                                                  index=['Program', 'Dept_Name'],
                                                  columns=['Year', 'Month'],
                                                  aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_fin_disch_dept.rename_axis(index={'Dept_Name': 'Department'}, inplace=True)
        return report_df_fin_disch_dept

    def fin_disch_ref_type(df_dc):
        report_df_fin_disch_ref_type = pd.pivot_table(df_dc, values='cnt',
                                                      index=['Program', 'Dept_Name', 'ref_type_fin'],
                                                      columns=['Year', 'Month'],
                                                      aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_fin_disch_ref_type.rename_axis(index={'Dept_Name': 'Department', 'ref_type_fin': 'Referral Type'},
                                                 inplace=True)
        return report_df_fin_disch_ref_type


class inflight_compute:

    #Generating Inflight reports
    # Section 1: 1-month rolling Inflight reports
    def daily_pt_days(df_inflight_period):
        unique_year_mth = df_inflight_period.loc[:, ['Year', 'Month']]
        unique_year_mth = unique_year_mth.drop_duplicates().sort_values(by=['Year', 'Month'], ascending=False)
        all_reports_df_daily_pt_days = []

        for index, row in unique_year_mth.iterrows():
            df_inflight_period_i = df_inflight_period.loc[(df_inflight_period['Year'] == row['Year']) &
                                                          (df_inflight_period['Month'] == row['Month'])]
            df_inflight_period_i["Month"] = df_inflight_period_i['Month'].apply(lambda x: calendar.month_abbr[x])
            df_inflight_period_i["Date"] = df_inflight_period_i['Inflight_Date'].dt.day
            report_df_daily_pt_days_i = pd.pivot_table(df_inflight_period_i, values='cnt', index=['Ward'],
                                                       columns=['Year', 'Month', 'Date'],
                                                       aggfunc=np.sum, margins=True, margins_name='Total')
            all_reports_df_daily_pt_days.append(report_df_daily_pt_days_i)
        return all_reports_df_daily_pt_days

    def df_lodger_pt_days(df_inflight_lodger, df_moh_speciality):
        report_df_lodger_pt_days = pd.pivot_table(df_inflight_lodger, values='cnt', index=['Moh_Clinical_Dept'],
                                                  columns=['Year', 'Month', 'Class_abc', 'Accom_Category'],
                                                  aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_lodger_pt_days = pd.merge(report_df_lodger_pt_days, df_moh_speciality, how='right',
                                            on='Moh_Clinical_Dept')
        report_df_lodger_pt_days.rename_axis(index={'Moh_Clinical_Dept': "MOH Clinical Department"},
                                             columns={'Class_abc': 'Patient Class',
                                                      'Accom_Category': 'Accommodation Class'}, inplace=True)
        return report_df_lodger_pt_days

    def F10_pt_days(df_inflight_lastMonth_w_dc):

        unique_year_mth = df_inflight_lastMonth_w_dc.loc[:, ['Year', 'Month']]
        unique_year_mth = unique_year_mth.drop_duplicates().sort_values(by=['Year', 'Month'], ascending=False)

        all_reports_df_F10_pt_days = []
        for index, row in unique_year_mth.iterrows():
            df_inflight_lastMonth_w_dc_i = df_inflight_lastMonth_w_dc.loc[(df_inflight_lastMonth_w_dc['Year'] == row['Year']) &
                                                                          (df_inflight_lastMonth_w_dc['Month'] == row['Month'])]
            df_inflight_lastMonth_w_dc_i["Month"] = df_inflight_lastMonth_w_dc_i['Month'].apply(lambda x: calendar.month_abbr[x])
            report_df_F10_pt_days_i = pd.pivot_table(df_inflight_lastMonth_w_dc_i, values='cnt',
                                                   index=['Moh_Clinical_Dept'],
                                                   columns=['Year', 'Month', 'Resident_Type', 'Class_icu_iso_MOH'],
                                                   aggfunc=np.sum, margins=True, margins_name='Total')
            report_df_F10_pt_days_i.rename_axis(index={'Moh_Clinical_Dept':'MOH Clinical Department'},
                                                columns={'Resident_Type': 'Resident Status',
                                                         'Class_icu_iso_MOH': 'Patient Class'}, inplace=True)
            all_reports_df_F10_pt_days.append(report_df_F10_pt_days_i)
        return all_reports_df_F10_pt_days

    # Section 2: 12-months rolling Inflight reports
    def fin_pt_days_class_abc(df_inflight_final):
        report_df_fin_pt_days_abc = pd.pivot_table(df_inflight_final, values='cnt',
                                                   index=['Program', 'Dept_Name', 'Class_abc'],
                                                   columns=['Year', 'Month'],
                                                   aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_fin_pt_days_abc.rename_axis(index={'Dept_Name': 'Department',
                                                     'Class_abc': "Patient Class"}, inplace=True)
        return report_df_fin_pt_days_abc

    def fin_pt_days_w_iso_HD(df_inflight_final):
        report_df_fin_pt_days_w_iso_HD = pd.pivot_table(df_inflight_final, values='cnt',
                                                index=['Program', 'Dept_Name', 'Class_with_icu_iso'],
                                                columns=['Year', 'Month'],
                                                aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_fin_pt_days_w_iso_HD.rename_axis(index={'Dept_Name': 'Department',
                                                          'Class_with_icu_iso': 'Class'},
                                                   inplace=True)
        return report_df_fin_pt_days_w_iso_HD

    def fin_pt_days_dept(df_inflight_final):
        report_df_fin_pt_days_dept = pd.pivot_table(df_inflight_final, values='cnt',
                                                    index=['Program', 'Dept_Name'],
                                                    columns=['Year', 'Month'],
                                                    aggfunc=np.sum, margins=True, margins_name='Total')
        report_df_fin_pt_days_dept.rename_axis(index={'Dept_Name': 'Department'}, inplace=True)
        return report_df_fin_pt_days_dept

    def pt_days_by_ward(df_inflight_final):
        report_df_pt_days_by_ward = pd.pivot_table(df_inflight_final, values='cnt',
                                                   index=['Ward'],
                                                   columns=['Year', 'Month'],
                                                   aggfunc=np.sum, margins=True, margins_name='Total')
        return report_df_pt_days_by_ward

