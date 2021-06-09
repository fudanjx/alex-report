import pandas as pd
import numpy as np
import path as PT
from calendar import monthrange


class acuity_compute:

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
        return final_df_ptdays_sum

    def sum_pt_days_dept_percent(pivot_ptdays_dept_df):
        df_percent = pivot_ptdays_dept_df.copy()
        for i in df_percent.columns:
            df_percent[i] = df_percent[i] / df_percent[i][len(
                df_percent) - 1]
            df_percent[i] = df_percent[i].astype(float).map("{:.1%}".format)
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
        return df_ALOS



class bis_compute:
    # reporting section for BIS
    def bis_by_class(df_bis_for_report):
        report_df_bis_by_class = pd.pivot_table(df_bis_for_report, values='BIS', index=['Class'],
                                                columns=['Year', 'Month'],
                                                aggfunc=np.sum, margins=True, margins_name='Total')
        return report_df_bis_by_class

    def bis_by_ward(df_bis_for_report):
        report_df_bis_by_ward = pd.pivot_table(df_bis_for_report, values='BIS', index=['Nrs_OU'],
                                               columns=['Year', 'Month'],
                                               aggfunc=np.sum, margins=True, margins_name='Total')
        return report_df_bis_by_ward

    def avg_bis_class(df_bis_for_report):
        report_df_bis_by_class_avg = pd.pivot_table(df_bis_for_report, values='BIS', index=['Class'],
                                                    columns=['Year', 'Month'],
                                                    aggfunc=np.sum).round(decimals=0)
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
