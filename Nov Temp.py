# -*- coding: utf-8 -*-
"""
Created on Thu Nov 28 14:35:03 2024

@author: Ahmad
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Nov 28 10:42:01 2024

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






ris1=pd.read_parquet(r'D:\AAML\CCC\Hospitals data\sample.parquet', engine='pyarrow')
#ris = pd.read_csv(r"D:\AAML\CCC\Hospitals data\ph_kf_yam_ar_dw_zu_mj19 Feb, 2024.csv")
#ris1 = pd.read_excel(r"D:\AAML\CCC\Hospitals data\ph_kf_yam_ar_dw_zu_mj_01 Sep, 2024.xlsx")

#invoice = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Invoices\Jul 2024 Procedures list_All Hospitals_v2.xlsx",sheet_name="Accessions")
#invoice = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Invoices\Aug 2024 Procedures list_All Hospitals_v1.xlsx",sheet_name="Accessions")
#invoice = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Invoices\Oct 2024 Procedures list_All Hospitals_v2.xlsx",sheet_name="Accessions")

#invoice = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Invoices\Reported Procedures - January 2024_PPP Radiology C2_AT.xlsx",sheet_name="Accessions")
# invoice = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Invoices\Reported Procedures - March 2024_PPP Radiology C2_AT.xlsx",sheet_name="Accessions")


# procduremapping_points = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\NPHIES Points System modified 21-4-2024.xlsx",sheet_name="KFMC to NICIP to NPHIES Mapping")

# procduremapping_points.info()

point_map = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\NPHIES Points System modified 21-4-2024.xlsx",sheet_name="Sheet1")

Reading_price = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Reading Fee_ Jan.24 Invoice v1.xlsx",sheet_name="Sheet3")


#roaster = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Rota\Dec.2023-Productivity-FINAL.xlsx")

#roaster = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Rota\JAN productivity-Final.xlsx")

#roaster = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Rota\RAD.DOC.-JULY 2024 PRODUCTIVITY.xlsx")
#roaster = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Rota\Radiology Doctors AUGUST 2024 Productivity.xlsx")
roaster = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\Rota\Novmber Temp.xlsx")


# roaster.columns=roaster.iloc[0]
# roaster=roaster[1:]



# roaster.replace(roaster['TOTAL ACTIVITIES'].str.startswith('-', na=False),np.nan ,inplace=True)
roaster.replace(r'^\s*$', np.nan, regex=True,inplace=True)

roaster.replace('____',np.nan ,inplace=True)
roaster.replace('__',np.nan,inplace=True)
roaster = roaster.dropna(subset=['ID No.'])
roaster['TOTAL ACTIVITIES']=roaster['TOTAL ACTIVITIES'].apply(lambda x: x if x >0 else 0)

roaster['workhours']=(roaster['No. of Workdays']*8)-roaster['TOTAL ACTIVITIES']
Radiolgistnames = pd.read_excel(r"D:\AAML\CCC\Hospitals data\ALL RADIOLOGOSITS MAPPED NAMES 15 May 24.xlsx")
roaster2=pd.merge(roaster, Radiolgistnames,left_on=roaster['Name'].str.upper().apply(lambda x: x.replace(' ','')),right_on=Radiolgistnames['Final unified list'].astype(str).str.upper().apply(lambda x: x.replace(' ','')),how="left")
roaster2=roaster2.drop(['key_0'],axis=1)
roaster2=roaster2.iloc[: , :-9]
educationhrs=56
northconsultant=['Dr. Fawzy Mohamed','Dr. Jaafar Abdul Rahman','Dr. Ahmed Ibrahim Abdel Aal','Dr. Ehab Ali Ahmed']
eduocationnormalize=.675
monthworkdays=max(roaster2['No. of Workdays'])
roaster2.loc[roaster2['Category']=="Consultant",'educations_hrs']=(roaster2['No. of Workdays']*educationhrs)/monthworkdays
roaster2.loc[roaster2['Admin']==1,'admin_hrs']=roaster2['No. of Workdays']*8*.2
roaster2.fillna(0,inplace=True)
roaster2.loc[roaster2['Name'].isin(northconsultant),'educations_hrs']=educationhrs/1.8
roaster2['net_report_hrs']=roaster2.workhours-roaster2.educations_hrs -roaster2.admin_hrs

roaster2['required_report_point']=roaster2.net_report_hrs*4
roaster2['required_eductio_point']=roaster2.educations_hrs *4*eduocationnormalize
roaster2['total_required_point']=roaster2.required_report_point +roaster2.required_eductio_point
roaster2.fillna(0)
roaster2=roaster2.drop_duplicates(['Name'])






























check=ris.loc[ris['PROCEDURE_KEY']=="Al Yamamah_20173937"]
check2=ris.loc[ris['Hospital']=="Al Artaweyyah"]

ris=ris1.copy()
ris.info()
outprocedur=['XR INTRAOPERATIVE','XR Dental Panoramic','MRI Contrast Material']
ris=ris.loc[~ris['PROCEDURE_NAME'].isin(outprocedur)]

ris['PROCEDURE_END']=pd.to_datetime(ris['PROCEDURE_END'],errors="coerce")
ris['REPORT_VERIFICATION_DATE']=pd.to_datetime(ris['REPORT_VERIFICATION_DATE'],errors="coerce")

startstr='11/01/24 00:00:01'
start = datetime.strptime(startstr, '%m/%d/%y %H:%M:%S')
endstr='11/30/24 23:59:59'
end = datetime.strptime(endstr, '%m/%d/%y %H:%M:%S')
ris.loc[ris['Hospital']=='Al Artaweyyah','PROCEDURE_KEY']=ris['PROCEDURE_KEY'].str.replace('.0','')
invoice['Acc_hospital']=invoice['Acc_hospital'].astype(str)
ris['PROCEDURE_KEY']=ris['PROCEDURE_KEY'].astype(str)
ris=ris.drop_duplicates(['PROCEDURE_KEY'])
ris_dec=pd.DataFrame()
ris_dec=ris.loc[(ris['REPORT_VERIFICATION_DATE'].between(start,end))]

ris[['PROCEDURE_KEY2','PROCEDURE_KEY3']]=ris['PROCEDURE_KEY'].str.split("_", n=1, expand=True)
ris.loc[ris['PROCEDURE_KEY3'].isnull(),'PROCEDURE_KEY3']=ris['PROCEDURE_KEY2']


#ris_dec=ris.loc[ris['PROCEDURE_KEY3'].str.upper().isin(invoice['Acc_hospital'].str.upper())]
ris_dec=ris_dec.drop_duplicates()
invtest=invoice.loc[~invoice['Acc_hospital'].isin(ris['PROCEDURE_KEY'])].dropna()
ris_dec['Hospital'].value_counts()

EidStart = date(2024,4,5)
EidEnd = date(2024,4,13)

#ris_dec=ris_dec.loc[((ris_dec['PROCEDURE_END'].dt.date< EidStart)&(ris_dec['PROCEDURE_END'].dt.date> EidEnd)&(ris_dec['REPORT_VERIFICATION_DATE'].dt.date< EidStart)&(ris_dec['REPORT_VERIFICATION_DATE'].dt.date> EidEnd))]






# ris_dec=ris.loc[(ris['PROCEDURE_END'].between(start,end))]
ris_dec=ris.loc[(ris['REPORT_VERIFICATION_DATE'].between(start,end))]

ris_dec.loc[(ris_dec['SIGNER_Name2']=='AHMAD ADNAN MOHAMMED ALDEREIHIM'),'SIGNER_Name2']='AHMED IBRAHIM ALDRAIHEM'

#ris_dec=ris_dec.iloc[:,0:70]

non_clinc_hours=0
Baseline_Points_hour=4
Baseline_Points_hour_xr=30
Baseline_Points_hour_mamo=20
normlization_mamo=5
normlization_xr=7.5
normalized_modality=['X-Ray','Mamo']
peditric_factor=.8
er_in_factor=.625
er_list=['Emergency',"E","InPatient","I"]

ris_dec["PROCEDURE_NAME_Nicp2"]=ris_dec["PROCEDURE_NAME_Nicp"].astype(str)

ris_dec["PROCEDURE_NAME_Nicp2"]=ris_dec["PROCEDURE_NAME_Nicp2"].str.upper()
ris_dec["PROCEDURE_NAME_Nicp2"]=ris_dec["PROCEDURE_NAME_Nicp2"].apply(lambda x: x.replace(' ','') )

point_map["NICIP Examination Name2"]=point_map["NICIP Examination Name"].astype(str).str.upper()
point_map["NICIP Examination Name2"]=point_map["NICIP Examination Name2"].apply(lambda x: x.replace(' ','') )

point_map=point_map.drop_duplicates(['NICIP Examination Name2'],keep="first")

ris_point=pd.merge(ris_dec, point_map,left_on=['PROCEDURE_NAME_Nicp2'],right_on=['NICIP Examination Name2']  ,how="left")
NotMappedPoint=ris_point.loc[ris_point['OPD 2024'].isnull()]
# ris_point['scanday']=ris_point['PROCEDURE_END'].dt.dayofweek
# ris_point['reportday']=ris_point['REPORT_VERIFICATION_DATE'].dt.dayofwee
ris_point['scan_weekday']=""
ris_point.loc[((ris_point['PROCEDURE_END'].dt.dayofweek==3)&(ris_point['PROCEDURE_END'].dt.time >= pd.to_datetime('16:30').time())),'scan_weekday']='Scanweekend'
ris_point.loc[((ris_point['PROCEDURE_END'].dt.dayofweek>3)&(ris_point['PROCEDURE_END'].dt.dayofweek<=5)),'scan_weekday']='Scanweekend'
ris_point['report_weekday']=""
ris_point.loc[((ris_point['REPORT_VERIFICATION_DATE'].dt.dayofweek==3)&(ris_point['REPORT_VERIFICATION_DATE'].dt.time >= pd.to_datetime('16:30').time())),'report_weekday']='Reportweekend'
ris_point.loc[((ris_point['REPORT_VERIFICATION_DATE'].dt.dayofweek>3)&(ris_point['REPORT_VERIFICATION_DATE'].dt.dayofweek<=5)),'report_weekday']='Reportweekend'
ris_point.loc[(ris_point['scan_weekday']=='Scanweekend')&(ris_point['report_weekday']=='Reportweekend'),'Weekend']=1


ris_point['scan_Bshift']=""
ris_point.loc[((ris_point['PROCEDURE_END'].dt.time >= pd.to_datetime('16:00').time())|(ris_point['PROCEDURE_END'].dt.time <= pd.to_datetime('7:00').time())),'scan_Bshift']='Scan_Bshift'
ris_point.loc[((ris_point['REPORT_VERIFICATION_DATE'].dt.time >= pd.to_datetime('16:00').time())|(ris_point['REPORT_VERIFICATION_DATE'].dt.time <= pd.to_datetime('7:00').time())),'report_Bshift']='Report_Bshift'
ris_point['TAT']=ris_point['REPORT_VERIFICATION_DATE']-ris_point['PROCEDURE_END']


filtered_df = ris_point.loc[(ris_point['PROCEDURE_END'].dt.weekday >= 3) & (ris_point['PROCEDURE_END'].dt.weekday <= 5) &((ris_point['PROCEDURE_END'].dt.weekday == 5) | (ris_point['PROCEDURE_END'].dt.time >= pd.to_datetime('16:30').time()))]

# ris_point.info()

# def calcpoint():
#     if ris_dec['Age']<=14:
#         ris_point['point']=Baseline_Points_hour/(ris_point['OPD 2024']*peditric_factor)
#     if ris_dec['ADMISSION_TYPE'].isin('Emergency',"E","InPatient","I"):
#         ris_point['point']=Baseline_Points_hour/(ris_point['OPD 2024']*er_in_factor)
#     return ris_point['point']
    
ris_point['point']=0
# ER & INPATIENT facor
ris_point.loc[(ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE'].str not in normalized_modality)),'point']=Baseline_Points_hour/(ris_point['OPD 2024']*er_in_factor)
ris_point.loc[(ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE']=="X-Ray")),'point']=(Baseline_Points_hour_xr/(ris_point['OPD 2024']*er_in_factor))/normlization_xr
ris_point.loc[(ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE']=="Mamo")),'point']=(Baseline_Points_hour_mamo/(ris_point['OPD 2024']*er_in_factor))/normlization_mamo

# # PED ER & INPATIENT
# ris_point.loc[(ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE'].str not in normalized_modality)&(ris_point['Age']<=14)),'point']=Baseline_Points_hour/(ris_point['OPD 2024']*peditric_factor*er_in_factor)
# ris_point.loc[(ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE']=="X-Ray")&(ris_point['Age']<=14)),'point']=(Baseline_Points_hour_xr/(ris_point['OPD 2024']*peditric_factor*er_in_factor))/normlization_xr
# ris_point.loc[(ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE']=="Mamo")&(ris_point['Age']<=14)),'point']=(Baseline_Points_hour_mamo /(ris_point['OPD 2024']*peditric_factor*er_in_factor))/normlization_mamo





# Out patient
ris_point.loc[(~ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE'].str not in normalized_modality)&(ris_point['Age']>14)),'point']=Baseline_Points_hour/(ris_point['OPD 2024'])
ris_point.loc[(~ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE']=="X-Ray")&(ris_point['Age']>14)),'point']=(Baseline_Points_hour_xr/(ris_point['OPD 2024']))/normlization_xr
ris_point.loc[(~ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE']=="Mamo")&(ris_point['Age']>14)),'point']=(Baseline_Points_hour_mamo /(ris_point['OPD 2024']))/normlization_mamo



# PED OPD
ris_point.loc[(~ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE'].str not in normalized_modality)&(ris_point['Age']<=14)),'point']=Baseline_Points_hour/(ris_point['OPD 2024']*peditric_factor)
ris_point.loc[(~ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE']=="X-Ray")&(ris_point['Age']<=14)),'point']=(Baseline_Points_hour_xr/(ris_point['OPD 2024']*peditric_factor))/normlization_xr
ris_point.loc[(~ris_point['ADMISSION_TYPE'].isin(['Emergency',"E","InPatient","I"])&(ris_point['SECTION_CODE']=="Mamo")&(ris_point['Age']<=14)),'point']=(Baseline_Points_hour_mamo/(ris_point['OPD 2024']*peditric_factor))/normlization_mamo
ris_point.info()







ris_point.loc[ ((ris_point['SIGNER_Name2']==ris_point['Assistant'])|(ris_point['Assistant'].isnull())),'Cons_point']=ris_point['point']
ris_point.loc[ ((ris_point['SIGNER_Name2']!=ris_point['Assistant'])&(~ris_point['Assistant'].isnull())),'Cons_point']=ris_point['point']*.6
ris_point.loc[ ((ris_point['SIGNER_Name2']!=ris_point['Assistant'])&(~ris_point['Assistant'].isnull())),'Assis_point']=ris_point['point']*.4
ris_point.loc[ris_point['Hospital']=='Al Artaweyyah','Hospital']='Al Artaweyah'
ris_point.loc[ris_point['Hospital']=='Al Majmaah','Hospital']='Al Majmah'
ris_point.info()

ris_point["Performing Technologist Name"]=ris_point["Performing Technologist Name"].str.upper()
ris_point['RadISTec']=0
    

ris_point.loc[(ris_point['Hospital']=="Al Zulfi")&(~ris_point['PROCEDURE_NAME'].str.contains("Obstetric" ,na=False,case=False))&(ris_point['SIGNER_Name']==ris_point['Performing Technologist Name'])&(ris_point['SECTION_CODE']=='US'),'RadISTec']=1
ris_point.loc[(ris_point['Hospital']=="Al Dawadmi")&(~ris_point['PROCEDURE_NAME'].str.contains("Obstetric", na=False,case=False))&(ris_point['SIGNER_Name']==ris_point['Performing Technologist Name'])&(ris_point['SECTION_CODE']=='US'),'RadISTec']=1
ris_point.loc[(ris_point['Hospital']=="Al Yamamah")&(~ris_point['PROCEDURE_NAME'].str.contains("OBSTETRIC", na=False,case=False))&(ris_point['ADMISSION_TYPE'].isin(["E","I"]))&(ris_point['SECTION_CODE']=='US'),'RadISTec']=1
ris_point.loc[ris_point['RadISTec']==1,'Cons_point']=ris_point['Cons_point']*2

    


    
    

# ris_point['Hospital_Proc']=ris_point['Hospital']+"_"+ris_point['PROCEDURE_CODE']

# Reading_price['Hospital_Proc']=Reading_price['Hospital']+"_"+Reading_price['Procedure ID']
# Reading_price.drop_duplicates(['Hospital_Proc'],inplace=True)
# Reading_price['Old Reading Price']=Reading_price['Reading Price']
# Reading_price['Reading Price']=Reading_price['Old Reading Price']*.9


ris_point=pd.merge(ris_point,invoice,left_on='PROCEDURE_KEY3',right_on='Acc_hospital',how='left')








ris_point.loc[ ((ris_point['SIGNER_Name2']==ris_point['Assistant'])|(ris_point['Assistant'].isnull())),'Cons_price']=ris_point['Reading Price']
ris_point.loc[ ((ris_point['SIGNER_Name2']!=ris_point['Assistant'])&(~ris_point['Assistant'].isnull())),'Cons_price']=ris_point['Reading Price']*.6
ris_point.loc[ ((ris_point['SIGNER_Name2']!=ris_point['Assistant'])&(~ris_point['Assistant'].isnull())),'Assis_price']=ris_point['Reading Price']*.4









ris_point['SIGNER_Name2']=ris_point['SIGNER_Name2'].str.strip()
# ris_point.columns

# RIS_GROUP=ris_point.groupby(['NICIP Examination Name','SECTION_CODE']).agg({'Hospital_x':'count' ,'OPD 2024':'max'}).reset_index()










radiolgist_time=ris_point.groupby(['SIGNER_Name2','SECTION_CODE'])['PROCEDURE_KEY'].count()  

assistant_time=ris_point.loc[ ((ris_point['SIGNER_Name2']!=ris_point['Assistant'])&(ris_point['Assistant'].astype(str)!='nan'))].groupby(['Assistant','SECTION_CODE'])['PROCEDURE_KEY'].count()  
assistant_time=ris_point.groupby(['Assistant','SECTION_CODE'])['PROCEDURE_KEY'].count()  

radiolgist_time.columns=['Radiologist','Modalitiy','Hospital','# Cases']
radiolgist_time=radiolgist_time.reset_index()

radioglist_list=ris_point['SIGNER_Name2'].drop_duplicates().dropna()
radioglist_list2=ris_point['Assistant'].drop_duplicates().dropna()
radioglist_list=pd.concat([radioglist_list,radioglist_list2]).str.strip().drop_duplicates().dropna()


radioglist_list.index=radioglist_list[:]
radioglist_list=radioglist_list.drop(labels =['SALEM AHMED BAUONES','ALIYA IBRAHIM ALAWAJI','Dr. Salah Mohammed Alhumaid NTRP','Dr. Ahmed Alrizqi','Dr. Nouman Hassan','Dr. Faisal Alfaisal','Dr. Saleh Alreshoodi','Dr. Tariq Alotaibi','Dr. Mohammed Alsabti','Dr. Waleed Althobaity','Dr. Hamad Aljubair','Dr. Maha Alhusain','Dr. Ibrahim Alturki','Dr. Rumian Alrumian','Dr. Mohammed Alahmadi','Dr. Saud Alawad','Dr. Reem Mahmoud','Dr. Asif Dar','Dr. Amal Alsaedi','Dr. Rawan Alharbi','Dr. Fares Garad','Dr. Asma Alakeel','Dr. Mohammed Qureshi','Dr. Salem Bauones','Abrar Alduraiby','Physician','Hissah Albilali','Norah Alsadhan','Sara ALbawardy','Sara  Mohammed Alghamdi','Dr. Musab Alamri','Dr. Halia Alshehri','Dr. Massod Jamil','OR User','Dr. Mohammed Alhowaish'],errors='ignore')


allproduc=pd.DataFrame(columns=['Radiologist','Total Point','Overtime Point','Hit target Date'])

radtotalpoints=pd.DataFrame()
rad_hos_moda=pd.DataFrame()
allapend4=pd.DataFrame()



i=1
for radiologist in radioglist_list:
  print(radiologist)
  consappend=pd.DataFrame()
  assisappend=pd.DataFrame()
  roaster2.fillna(0)
  consappend=pd.DataFrame()
  assisappend=pd.DataFrame() 
  consappend2=pd.DataFrame()
  assisappend2=pd.DataFrame()
  
  roasterradio=roaster2.loc[roaster2['Name'].astype(str).str.upper().apply(lambda x: x.replace(' ',''))==radiologist.upper().replace(' ','')]
  
  # Week days
  cons2=ris_point.loc[(ris_point['SIGNER_Name2'].astype(str).str.upper().apply(lambda x: x.replace(' ',''))==radiologist.upper().replace(' ',''))]
  consappend2=pd.concat([consappend2,cons2],ignore_index=True, sort=False)
  
  assis2=ris_point.loc[(ris_point['Assistant'].astype(str).str.upper().apply(lambda x: x.replace(' ',''))==radiologist.upper().replace(' ',''))&(ris_point['SIGNER_Name2'].astype(str).str.upper().apply(lambda x: x.replace(' ',''))!=radiologist.upper().replace(' ',''))]
  assisappend2=pd.concat([assisappend2,assis2],ignore_index=True, sort=False)
  
  
  ris_point['SECTION_CODE'].value_counts()
  
  
  
  
  
 #print(len(roasterradio.iloc[0, 9]))
  if ((len(roasterradio)>0)):
      allapend=weekend(radiologist, roasterradio, ris_point)
      thursday=thursday_afterhours(radiologist, roasterradio, ris_point)
      er_reportinf=er_reporting(radiologist, roasterradio, ris_point)
      extra_shifts=extrashifts_assist(radiologist, roasterradio, ris_point)
      Overtime=pd.concat([allapend,thursday,er_reportinf,extra_shifts]) 


  else:
      continue
      
      
  #allapend.to_excel(r'D:\AAML\CCC\Hospitals data\Radiologist Productivity\Weekend '+radiologist+'.xlsx', sheet_name = "All", index = False) 
  
  consappend2['Class']='solo management'
  consappend2.rename(columns={'Cons_point':'Earned_point'},inplace=True)
  consappend2.rename(columns={'Cons_price':'Earned_M'},inplace=True)

  assisappend2['Class']='Under Supervision'
  assisappend2.rename(columns={'Assis_point':'Earned_point'},inplace=True)
  assisappend2.rename(columns={'Assis_price':'Earned_M'},inplace=True)

  if(len(assisappend2)>0):
       allapend2=pd.concat([consappend2,assisappend2])
  else:
       allapend2=consappend2

  allapend2['day']='WeekDay'
  if(len(Overtime)>0):
      allapend2=allapend2.loc[~allapend2['PROCEDURE_KEY'].isin(Overtime['PROCEDURE_KEY'])]
  allapend2.drop_duplicates(['PROCEDURE_KEY'],inplace=True)

#     merged_df = pd.merge(allapend2, allapend, on='PROCEDURE_KEY', how='left', indicator=True)

# # Filter out the rows from df1 that are not in df2
#     filtered_df = merged_df[merged_df['_merge'] == 'left_only']

# # Drop the '_merge' column
#     filtered_df = filtered_df.drop('_merge', axis=1)
#     allapend2=filtered_df
  
  
  
  
  
  overtime_classes=['WeekEnd','Thursday_afterHours','Extra Shifts','ER REPORTING']
  if(len(Overtime)>0):
      Overtime.loc[Overtime['day']=='WeekEnd','Accu_M_end']=Overtime.loc[Overtime['day'] =='WeekEnd', 'Earned_M'].cumsum()
      Overtime.loc[Overtime['day']=='Thursday_afterHours','Accu_M_end']=Overtime.loc[Overtime['day'] =='Thursday_afterHours', 'Earned_M'].cumsum()
      Overtime.loc[Overtime['day']=='Extra Shifts','Accu_M_end']=Overtime.loc[Overtime['day'] =='Extra Shifts', 'Earned_M'].cumsum()
      Overtime.loc[Overtime['day']=='ER REPORTING','Accu_M_end']=Overtime.loc[Overtime['day'] =='ER REPORTING', 'Earned_M'].cumsum()
  else:
       Overtime['Accu_M_end']=0
      
       
      
    
  

  
  
  
  
  allapend3=pd.concat([Overtime,allapend2]) 
  allapend3 = allapend3.reset_index(drop=True)
  allapend3.sort_values('REPORT_VERIFICATION_DATE')
  allapend3.loc[allapend3['day']=="WeekDay",'Accu_point']=allapend3.loc[allapend3['day'] == "WeekDay", 'Earned_point'].cumsum()
  #allapend3.loc[allapend3['day'].isin(overtime_classes),'Accu_M_end']=allapend3.loc[allapend3['day'] .isin(overtime_classes), 'Earned_M'].cumsum()

  allapend3.loc[allapend3['day']=="WeekDay",'Accu_M']=allapend3.loc[allapend3['day'] == "WeekDay", 'Earned_M'].cumsum()
  # allapend3['Accu_point']=allapend3['Earned_point'].cumsum()

  allapend3.loc[allapend3['Accu_point']>roasterradio ['total_required_point'].sum(),'Accu_M_day']=allapend3.loc[allapend3['Accu_point']>roasterradio ['total_required_point'].sum(),'Earned_M'].cumsum()
  
  # allapend3.loc[allapend3['day']=="WeekDay",'Accu_point']= allapend3['Earned_point'].cumsum()
  # allapend3.loc[allapend3['day'].isin(overtime_classes),'Accu_M_end']=allapend3[ 'Earned_M'].cumsum()

  # allapend3.loc[allapend3['day']=="WeekDay",'Accu_M']=allapend3[ 'Earned_M'].cumsum()
  # #allapend3['Accu_point']=allapend3['Earned_point'].cumsum()

  # allapend3.loc[allapend3['Accu_point']>roasterradio ['total_required_point'].sum(),'Accu_M_day']=allapend3['Earned_M'].cumsum()
  
  
  
  
  final=allapend3.groupby(['Class','day']).agg({'PROCEDURE_KEY':'count' ,'Earned_point':'sum','Accu_M_day':['max','count'],'Accu_M_end':'max'})
  final.columns = final.columns.map('_'.join).str.strip('_')
  final1= final.reset_index()
  final2=allapend3.groupby(['Hospital','SECTION_CODE','day']).agg({'PROCEDURE_KEY':'count' ,'Earned_point':'sum'})
  final2.columns = final2.columns.map('_'.join).str.strip('_')
  final2= final2.reset_index()
  final2['Radiolgist']=radiologist

  allapend4=pd.concat([allapend4,allapend3])

  final1['Radiolgist']=radiologist
  
  mus=ris_dec.loc[ris_dec['SIGNER_Name2']=='Dr. Moustafa Gaber']
  
  
  
  radtotalpoints=pd.concat([radtotalpoints,final1])
  rad_hos_moda=pd.concat([rad_hos_moda,final2])
  radtotalpoints['Month']=end
  rad_hos_moda['Month']=end

  #allapend3.to_excel(r'D:\AAML\CCC\Hospitals data\Radiologist Productivity\Weekend '+radiologist+'.xlsx', sheet_name = "All", index = False) 
  
  
  i+=1
  
  # # # # #  # if i > 1: 
  # # # # #  #   break
  if i > 51: 
  
    break
fin=pd.merge(radtotalpoints, roaster2,left_on='Radiolgist',right_on='Name',how="left")
fin.info()
fin.rename(columns={'Hospital_x_count':'no._cases','Earned_point_sum':'total_point','Accu_M_day_max':'Ot_weekday_sr','Accu_M_day_count':'ot_weekday_cases','Accu_M_end_max':'Ot_weekend_sr',},inplace=True)
fin['Overtime']=0
fin.loc[fin['day']=='WeekDay','Overtime']=fin['total_point']-fin['total_required_point']
fin.to_excel(r'D:\AAML\CCC\Hospitals data\Radiologist Productivity\Radpoins_Sep_26Nov24.xlsx', sheet_name = "All", index = False)
rad_hos_moda.to_excel(r'D:\AAML\CCC\Hospitals data\Radiologist Productivity\Radstats_Sep_26Nov24.xlsx', sheet_name = "All", index = False)


ris_point.to_excel(r'D:\AAML\CCC\Hospitals data\Radiologist Productivity\risall_Sep.xlsx', sheet_name = "All", index = False)
