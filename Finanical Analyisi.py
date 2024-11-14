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



Salary['Month']=pd.to_datetime(Salary['Month']).dt.date
Stats['Month']=pd.to_datetime(Stats['Month']).dt.date
Overtime['Month']=pd.to_datetime(Overtime['Month']).dt.date


# Stats file data mainpulation 

Stats2=Stats.groupby(['Radiolgist','Month'])['E_a_r_n_e_d___p_o_i_n_t'].sum()
Stats2=Stats2.reset_index()
Stats3=pd.merge(Stats,Stats2,on=['Radiolgist','Month'],how='left')
Stats3['Point%']=Stats3['E_a_r_n_e_d___p_o_i_n_t_x']/Stats3['E_a_r_n_e_d___p_o_i_n_t_y']




# Overtime data mainpulation 

Overtime2=Overtime.groupby(['ID No.','Radiolgist','Month']).agg({'no._cases':'sum' ,'total_point':'sum','Ot_weekday_sr':['max'],'Ot_weekend_sr':'sum'})
Overtime2.columns = Overtime2.columns.map('_'.join).str.strip('_')
Overtime2= Overtime2.reset_index()
Overtime2=Overtime2.fillna(0)

Overtime2['TotalOvertime']=Overtime2['Ot_weekday_sr_max']+Overtime2['Ot_weekend_sr_sum']




# Suffix tables
#Radiologist_master = Radiologist_master.add_su ('Master')




# Join master with salary
RadWithSalary=pd.merge(Radiologist_master,Salary,left_on="ID No.",right_on="Code",how="left")
SalaryRad=pd.merge(Radiologist_master,Salary,left_on="ID No.",right_on="Code",how="right")

InMasterNotSalary=RadWithSalary.loc[RadWithSalary['Total'].isnull()]
InSalaryNotMaster=SalaryRad.loc[SalaryRad['Name'].isnull()]

RadWithSalary['ID No.']=RadWithSalary['ID No.'].astype(str)
Overtime2['ID No.']=Overtime2['ID No.'].astype(str)



# Add Stats3 to the join
SalaWithStat=pd.merge(RadWithSalary,Stats3,left_on=["Name","Month"],right_on=["Radiolgist","Month"],how="left")
StatWithSala=pd.merge(RadWithSalary,Stats3,left_on=["Name","Month"],right_on=["Radiolgist","Month"],how="right")

InSalaNotStat=SalaWithStat.loc[(SalaWithStat['Point%'].isnull())&(SalaWithStat['Total']>0)]
InSataNotStal=StatWithSala.loc[(StatWithSala['Total'].isnull())&(StatWithSala['Point%'].notnull())]

# Add Overtime to the join
SalaWithStat['ID No.']=SalaWithStat['ID No.'].str.strip()

Overtime2['ID No.']=Overtime2['ID No.'].str.strip()
AllJoin=pd.merge(SalaWithStat,Overtime2,on=["ID No.","Month"],how="left")
#StatWithSala=pd.merge(RadWithSalary,Stats3,left_on=["Name","Month"],right_on=["Radiolgist","Month"],how="right")
AllJoin['SalaryCost']=AllJoin['Total']*AllJoin['Point%']
AllJoin['OvertimeCost']=AllJoin['TotalOvertime']*AllJoin['Point%']
AllJoin['TotalCost']=AllJoin['SalaryCost']+AllJoin['OvertimeCost']
AllJoin=AllJoin.fillna(0)

AllJoin.to_excel(r'C:\Users\Ahmad\Desktop\clincal cost\Model/CostAnalysis.xlsx', sheet_name = "All", index = False)

yaseer=AllJoin.loc[AllJoin['Radiolgist_x']!=AllJoin['Radiolgist_y']]

InSalaNotStat=SalaWithStat.loc[(SalaWithStat['Point%'].isnull())&(SalaWithStat['Total']>0)]
InSataNotStal=StatWithSala.loc[(StatWithSala['Total'].isnull())&(StatWithSala['Point%'].notnull())]





