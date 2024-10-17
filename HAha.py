# -*- coding: utf-8 -*-
"""
Created on Mon Oct 14 13:57:52 2024

@author: Ahmad
"""

import pandas as pd
import re
from datetime import date, time

########################################################################
# STEP 1:
    points_df = pd.read_excel('NPHIES Points System modified 21-4-2024.xlsx', sheet_name='Sheet1')
    points_df['NICIP Examination Name Formatted'] = points_df['NICIP Examination Name'].apply(lambda x: ''.join(x.upper().split()))
    points_map = dict(points_df.set_index('NICIP Examination Name Formatted')['OPD 2024'])

STEP 2:
    financial_data = pd.read_excel('financial.xlsx')[['Procedure ID', 'Reading Price']].drop_duplicates()

STEP 3:
    emarat_details = {
        'weekend_reporting': [(date(2024, 5, 16), 'MRI')],
        'er_reporting': [],
        'admin_hours': 0,
        'non_clinical_hours': 4,
        'number_of_workdays': 22,
        'signature': 'HUSSAIN'
    }
########################################################################

def factor_opd(row):
    type = row['ADMISSION_TYPE']
    opd = row['opd2024']

    if row['Age'] <= 14:
        return opd * 0.8
    else:
        if type in ('ER', 'INPT'):
            return opd * 0.62
    return opd

def get_price(row):
    price = financial_data[financial_data['Procedure ID']==row['PROCEDURE_CODE']]['Reading Price'].values[0]
    if row['overtime'] == 1:
        return price * 0.9
    return

def check_weekend(row):
    if row['PROCEDURE_END'].day_name() in ('Friday', 'Saturday') or \
    (row['PROCEDURE_END'].time() >= time(16) and row['PROCEDURE_END'].day_name() == 'Thursday'):
        return 1
    return 0

def check_workday(row):
    if row['PROCEDURE_END'].day_name() not in ('Friday', 'Saturday') and \
    not (row['PROCEDURE_END'].time() >= time(16) and row['PROCEDURE_END'].day_name() == 'Thursday'):
        return 1
    return 0

def get_threshold(doctor_details, hours_per_day=8, points_per_hour=4, training_hours=56, training_factor=0.675):

    admin_hours = doctor_details['admin_hours'] * (0.2 * doctor_details['number_of_workdays'] * 8)
    total_hours = (doctor_details['number_of_workdays'] * hours_per_day) - (admin_hours + doctor_details['non_clinical_hours'])
    total_working_hours = total_hours - training_hours
    total_training_hours = training_hours * training_factor

    threshold = (total_working_hours + total_training_hours) * 4

    return threshold

def get_roster_overtime(row, doctor_details):
    
    for date, modality in doctor_details['weekend_reporting']:
        # if row['REPORT_VERIFICATION_DATE'].date() == date and \
        if row['PROCEDURE_END'].date() == date and \
        modality in row['SECTION_CODE'] and \
        row['weekend'] == 1:
            return 1
            
    for date in doctor_details['er_reporting']:
        # if row['REPORT_VERIFICATION_DATE'].date() == date and \
        if row['PROCEDURE_END'].date() == date and \
        row['ADMISSION_TYPE'] == 'ER' and \
        row['weekend'] == 1:
            return 1
    return 0

def format_data(df, consultant, points_map, doctor_details, threshold):
    signature = doctor_details['signature']
    
    df['opd2024'] = df['PROCEDURE_NAME_Nicp'].astype(str).apply(lambda x: ''.join(x.upper().split())).map(points_map)
    df['opd_after_factor'] = df.apply(lambda x: factor_opd(x), axis=1)
    df['points'] = round(4 / df['opd_after_factor'], 2)
    # df['report_day_of_week'] = df['REPORT_VERIFICATION_DATE'].dt.day_name()
    # df['report_hour_of_day'] = df['REPORT_VERIFICATION_DATE'].dt.time
    df['procedure_day_of_week'] = df['PROCEDURE_END'].dt.day_name()
    df['procedure_hour_of_day'] = df['PROCEDURE_END'].dt.time
    df['workday'] = df.apply(lambda x: check_workday(x), axis=1)
    df['weekend'] = df.apply(lambda x: check_weekend(x), axis=1)

    if consultant:
        df['Assistant'] = df['Assistant'].fillna('None').str.split().apply(lambda x: x[-1]).str.upper()
        df['Assistant'] = np.where(df['Assistant'] == signature, 'NONE', df['Assistant'])

        df['point_share'] = df['points'] * np.where(
                                                    df['Assistant'] == 'NONE',
                                                    1,
                                                    0.6)
    
    else:
        df['point_share'] = df['points'] * np.where(
                                                    df['SIGNER_Name'].str.split().apply(lambda x: x[-1]) == signature,
                                                    1,
                                                    0.4)

    df_end = df[df['weekend'] == 1]

    # df_end['roster'] = df_end.apply(lambda x: get_weekend_overtime(x, doctor_details), axis=1)
    # df_roster = df_end[df_end['roster'] == 1]
    # df_end = df_end[df_end['roster'] == 0]

    
    # df_end['points_cumsum'] = df_end['points'].cumsum()
    
    df_work = df[df['workday'] == 1]
    df_work['points_cumsum'] = df_work['point_share'].cumsum()

    # df_roster = df[df['roster'] == 1]

    df_work['overtime'] = np.where(df_work['points_cumsum'] > threshold, 1, 0)
    threshold = threshold - df_work['point_share'].sum()

    df_end['overtime'] = df_end.apply(lambda x: get_roster_overtime(x, doctor_details), axis=1)
    df_roster = df_end[df_end['overtime'] == 1]
    df_end = df_end[df_end['overtime'] == 0]

    df_end['points_cumsum'] = df_end['point_share'].cumsum()

    df_end['overtime'] = np.where(df_end['points_cumsum'] > threshold, 1, 0)


    df_work['price'] = df_work.apply(lambda x: get_price(x), axis=1)
    df_end['price'] = df_end.apply(lambda x: get_price(x), axis=1)
    df_roster['price'] = df_roster.apply(lambda x: get_price(x), axis=1)

    # print('Total number of points in work days:', df_work['point_share'].sum())
    # print('Total number of points in weekend days:', df_end['point_share'].sum())
    # print('Total number of points in roster days:', df_roster['point_share'].sum())
    return df_work, df_end, df_roster


# TO EXPORT: 
    # with pd.ExcelWriter('emarat_results.xlsx', engine='xlsxwriter') as writer:
    #     # Write each DataFrame to a different worksheet.
    #     emarat_work.to_excel(writer, sheet_name='Workdays')
    #     emarat_end.to_excel(writer, sheet_name='Weekends')
    #     emarat_roster.to_excel(writer, sheet_name='Roster')