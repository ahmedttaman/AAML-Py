# -*- coding: utf-8 -*-
"""
Created on Sat Nov  9 14:43:23 2024

@author: Ahmad
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
import os
import datetime as dt
from ramadan_func_July import *
import pyarrow as pa
import pyarrow.parquet as pq



# Read master data
Radiologist_master = pd.read_excel(r"C:\Users\Ahmad\Desktop\clincal cost\Model\Radiologist Master.xlsx",sheet_name="Master")
Salary = pd.read_excel(r"C:\Users\Ahmad\Desktop\clincal cost\Model\Radiologist Master.xlsx",sheet_name="May Salary")
Stats = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\’Master Radstatcs.xlsx",sheet_name="All")
Overtime = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Master Radpoints.xlsx",sheet_name="All")

Overtime.info()
Radiologist_master.rename(columns={'ID No.': 'Master_Code', 'Name': 'Master_Name'}, inplace=True)
Salary.rename(columns={'Code': 'Salary_Code', 'Employee Name': 'Salary_Name','Hospital':'Main_Hospital','Month':'Salary_Month','Total':'Salary'}, inplace=True)
Stats.rename(columns={'Hospital_x': 'Stats_Hospital', 'SECTION_CODE': 'Stats_SECTIONCODE','H_o_s_p_i_t_a_l___x':'Stats_NoCases','E_a_r_n_e_d___p_o_i_n_t':'Stats_EarnedPoint','Radiolgist':'Stats_Name','Month':'Stats_Month'}, inplace=True)
Overtime.rename(columns={'Radiolgist': 'OT_Name', 'Month': 'OT_Month','ID No.':'OT_Code'}, inplace=True)




Salary=Salary.drop(['S','Modality','Dep.','Allocation'],axis=1)
Stats=Stats.drop(['Unnamed: 7'],axis=1)
Overtime=Overtime.drop(['Overtime','Unnamed: 0','Extra Shifts for  Assistant','Remarks','Name','Admin','ER REPORTING','Thursday coverage','Weekend Reporting   (Friday-Saturday)','No. of Workdays',],axis=1)




Salary['Salary_Month']=pd.to_datetime(Salary['Salary_Month']).dt.date
Stats['Stats_Month']=pd.to_datetime(Stats['Stats_Month']).dt.date
Overtime['OT_Month']=pd.to_datetime(Overtime['OT_Month']).dt.date


# Stats file data mainpulation 


Stats2=Stats.groupby(['Stats_Name','Stats_Month']).agg({'Stats_EarnedPoint':'sum','Stats_NoCases':'sum'})
Stats2=Stats2.reset_index()
Stats2.rename(columns={'Stats_EarnedPoint': 'Stats_TotalEarnedPoint', 'Stats_NoCases': 'Stats_TotalCases'}, inplace=True)

Stats3=pd.merge(Stats,Stats2,on=['Stats_Name','Stats_Month'],how='left')
Stats3['Point%']=Stats3['Stats_EarnedPoint']/Stats3['Stats_TotalEarnedPoint']



#stats4vacction=Stats.groupby(['Radiolgist','Hospital_x','SECTION_CODE'])['E_a_r_n_e_d___p_o_i_n_t'].sum()





# Overtime data mainpulation 

Overtime2=Overtime.groupby(['OT_Code','OT_Name','OT_Month']).agg({'Ot_weekday_sr':['max'],'Ot_weekend_sr':'sum','total_required_point':'max'})
Overtime2.columns = Overtime2.columns.map('_'.join).str.strip('_')
Overtime2= Overtime2.reset_index()
Overtime2=Overtime2.fillna(0)

Overtime2['TotalOvertimeSR']=Overtime2['Ot_weekday_sr_max']+Overtime2['Ot_weekend_sr_sum']




# Suffix tables
#Radiologist_master = Radiologist_master.add_su ('Master')




# Join master with salary
RadWithSalary=pd.merge(Radiologist_master,Salary,left_on="Master_Code",right_on="Salary_Code",how="left")
SalaryRad=pd.merge(Radiologist_master,Salary,left_on="Master_Code",right_on="Salary_Code",how="right")

InMasterNotSalary=RadWithSalary.loc[RadWithSalary['Salary'].isnull()]
InSalaryNotMaster=SalaryRad.loc[SalaryRad['Master_Name'].isnull()]

RadWithSalary['Master_Code']=RadWithSalary['Master_Code'].astype(str)
Overtime2['OT_Code']=Overtime2['OT_Code'].astype(str)



# Add Stats3 to the join
SalaWithStat=pd.merge(RadWithSalary,Stats3,left_on=["Master_Name","Salary_Month"],right_on=["Stats_Name","Stats_Month"],how="left")
StatWithSala=pd.merge(RadWithSalary,Stats3,left_on=["Master_Name","Salary_Month"],right_on=["Stats_Name","Stats_Month"],how="right")

InSalaNotStat=SalaWithStat.loc[(SalaWithStat['Point%'].isnull())&(SalaWithStat['Salary']>0)]


InSataNotStal=StatWithSala.loc[(StatWithSala['Salary'].isnull())&(StatWithSala['Point%'].notnull())] 333333

# Add Overtime to the join
SalaWithStat['Master_Code']=SalaWithStat['Master_Code'].str.strip()

Overtime2['OT_Code']=Overtime2['OT_Code'].str.strip()
AllJoin=pd.merge(SalaWithStat,Overtime2,left_on=["Master_Code","Salary_Month"],right_on=['OT_Code',"OT_Month"],how="left")
#StatWithSala=pd.merge(RadWithSalary,Stats3,left_on=["Name","Month"],right_on=["Radiolgist","Month"],how="right")
AllJoin['SalaryCost']=AllJoin['Salary']*AllJoin['Point%']
AllJoin['OvertimeCost']=AllJoin['TotalOvertimeSR']*AllJoin['Point%']
AllJoin['TotalCost']=AllJoin['SalaryCost']+AllJoin['OvertimeCost']
AllJoin.loc[(AllJoin['Salary']>0)&(AllJoin['Stats_Hospital'].isnull())&(AllJoin['Stats_SECTIONCODE'].isnull()),'SalaryCost']=AllJoin['Salary']
AllJoin.loc[(AllJoin['Salary']>0)&(AllJoin['Stats_Hospital'].isnull())&(AllJoin['Stats_SECTIONCODE'].isnull()),'TotalCost']=AllJoin['Stats_SECTIONCODE']


AllJoin=AllJoin.fillna(0)
AllJoin.info()
AllJoin

AllJoin.to_excel(r'C:\Users\Ahmad\Desktop\clincal cost\Model/CostAnalysis.xlsx', sheet_name = "All", index = False)

yaseer=AllJoin.loc[AllJoin['Radiolgist_x']!=AllJoin['Radiolgist_y']]

InSalaNotStat=SalaWithStat.loc[(SalaWithStat['Point%'].isnull())&(SalaWithStat['Total']>0)]
InSataNotStal=StatWithSala.loc[(StatWithSala['Total'].isnull())&(StatWithSala['Point%'].notnull())]





