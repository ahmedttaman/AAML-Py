# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 14:07:18 2024

@author: Ahmedtaman
"""

# -*- coding: utf-8 -*-
"""
Created on Sat May  4 16:18:04 2024

@author: Ahmedtaman
"""


import pandas as pd
import numpy as np
from datetime import datetime, date
import os
import datetime as dt
from Point_functions import *




#############################master Tables

import calendar

def get_weekends(year, month):
    cal = calendar.Calendar(firstweekday=calendar.SUNDAY)
    month_matrix = cal.monthdatescalendar(year, month)
    
    weekends = []
    for week in month_matrix:
        for day in week:
            if day.weekday() == calendar.FRIDAY or day.weekday() == calendar.SATURDAY:
                weekends.append(day)
    
    return weekends
weekend_dates=get_weekends(2023, 12)











#ris = pd.read_csv(r"D:\AAML\CCC\Hospitals data\ph_kf_yam_ar_dw_zu_mj19 Feb, 2024.csv")
ris1 = pd.read_excel(r"D:\AAML\CCC\Hospitals data\ph_kf_yam_ar_dw_zu_mj_02 Jun, 2024.xlsx")
invoice = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Invoices\All Invoices.xlsx",sheet_name="Accessions")


ris=ris1.copy()
#ris['PROCEDURE_END']=pd.to_datetime(ris['PROCEDURE_END'],errors="coerce")
ris['REPORT_VERIFICATION_DATE']=pd.to_datetime(ris['REPORT_VERIFICATION_DATE'],errors="coerce")

startstr='05/01/24 00:00:01'
start = datetime.strptime(startstr, '%m/%d/%y %H:%M:%S')
endstr='05/31/24 23:59:59'
end = datetime.strptime(endstr, '%m/%d/%y %H:%M:%S')
ris.loc[ris['Hospital']=='Al Artaweyyah','PROCEDURE_KEY']=ris['PROCEDURE_KEY'].str.replace('.0','')

#invtest=invoice.loc[~invoice['Acc_hospital'].isin(ris['PROCEDURE_KEY'])].dropna()

#ris_month=ris.loc[ris['PROCEDURE_KEY'].isin(invoice['Acc_hospital'])]



#ris_month=ris.loc[(ris['PROCEDURE_END'].between(start,end))]

ris_month=ris.loc[(ris['REPORT_VERIFICATION_DATE'].between(start,end))]
ris_month['Hospital'].value_counts()

outprocedur=['XR INTRAOPERATIVE','XR Dental Panoramic','MRI Contrast Material','RAD OUTSIDE CD FOR REVIEW']


ris_month.to_excel(r'D:\AAML\CCC\Hospitals data\Radiologist Productivity\Invoices\May V1.xlsx', sheet_name = "All", index = False)

