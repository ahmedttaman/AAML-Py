# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 12:08:19 2023

@author: Ahmedtaman
"""


import pandas as pd
import numpy as np
from datetime import datetime, date
import os
import datetime as dt



#############################master Tables
Old_data = pd.read_excel(r"D:\AAML\CCC\Hospitals data\ph_kf_yam_ar_dw_zu_mj_29 Oct, 2024.xlsx")

startstr='09/01/24 00:00:00'
start = datetime.strptime(startstr, '%m/%d/%y %H:%M:%S')





Old_data=Old_data.loc[Old_data['PROCEDURE_END']<start]
Radiolgistnames = pd.read_excel(r"D:\AAML\CCC\Hospitals data\ALL RADIOLOGOSITS MAPPED NAMES Sep.xlsx")
Radiolgistnames.fillna(0,inplace=True)

mapping = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Mapping\hospital mapping.xlsx")

procduremapping_points = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\NPHIES Points System modified 21-4-2024.xlsx",sheet_name="KFMC to NICIP to NPHIES Mapping")

pmah_stafff= pd.read_excel(r"D:\AAML\CCC\Hospitals data\PMAH\PMAH 10 Nov Staff .xlsx",sheet_name="Sheet1")
alyma_stafff= pd.read_excel(r"D:\AAML\CCC\Hospitals data\AlYamamh\C2 10 Nov STAFF.xlsx",sheet_name="Sheet1")

#procduremapping_points.info()




###################PMAH clen and compine data######################################
phma_rawperformed = pd.read_excel(r"D:\AAML\CCC\Hospitals data\PMAH\All performed PMAH 01 Sep 2024 to 16 Nov 2024.xlsx")
phma_rawreported = pd.read_excel(r"D:\AAML\CCC\Hospitals data\PMAH\All reported PMAH 01 Sep 2024 to 16 Nov 2024 .xlsx")
phma_rawperformed.info()

###  in performed but not in reported 

phma_rawperformed_w_duplic=phma_rawperformed.loc[~phma_rawperformed['PROCEDURE_KEY'] .isin( phma_rawreported['PROCEDURE_KEY'])]


# Performed and reported 
phma_rawperformed_reported=phma_rawperformed.loc[phma_rawperformed['PROCEDURE_KEY'] .isin( phma_rawreported['PROCEDURE_KEY'])]




diffrence=phma_rawreported.loc[phma_rawreported['PROCEDURE_KEY'] .isin( phma_rawperformed_reported['PROCEDURE_KEY'])]
phma_rawreporteddup=phma_rawreported[phma_rawreported['PROCEDURE_KEY'].duplicated(keep=False)==True]

phma_rawperformdup=phma_rawperformed[phma_rawperformed['PROCEDURE_KEY'].duplicated(keep=False)==True]












phah_combined=pd.concat([phma_rawreported, phma_rawperformed_w_duplic], ignore_index=True)

#phah_combined=phah_combined[phah_combined["SIGNER_CODE"]=="2932"]

##### Hisptorical data part

# phma_nov = pd.read_excel(r"D:\AAML\CCC\Hospitals data\PMAH\Nov PMAH contract modality.xlsx")

# phma_nov_w_duplic=phma_nov.loc[~phma_nov['PROCEDURE_KEY'] .isin( phah_combined['PROCEDURE_KEY'])]

#phah_combined=pd.concat([phah_combined, phma_nov_w_duplic], ignore_index=True)

###########################################



phah_combined['PAT_NAME']=phah_combined['PAT_FIRST_NAME']+" "+phah_combined['PAT_LAST_NAME']

phah_combined['PP_MISC_TEXT_22']=phah_combined['PP_MISC_TEXT_2'].astype(str).str.split(pat=".").str[0]

phah_combined['PP_MISC_TEXT_23']=pd.to_datetime(phah_combined['PP_MISC_TEXT_22'].str[:4]   +"-"+phah_combined['PP_MISC_TEXT_22'].str[4:6]  +"-"+   phah_combined['PP_MISC_TEXT_22'].str[6:8] +" "+   phah_combined['PP_MISC_TEXT_22'].str[8:10] +":"+   phah_combined['PP_MISC_TEXT_22'].str[10:12]+":"+   phah_combined['PP_MISC_TEXT_22'].str[-2:],errors='coerce')


phah_combined=pd.merge(phah_combined,pmah_stafff,left_on='SIGNER_CODE',right_on='STAFF_MEMB_CODE',how='left')
phah_combined=pd.merge(phah_combined,pmah_stafff,left_on='PERFORMING_DOCTOR',right_on='STAFF_MEMB_CODE',how='left')
phah_combined=pd.merge(phah_combined,pmah_stafff,left_on='COREADER_CODE',right_on='STAFF_MEMB_CODE',how='left')


phah_combined.rename(columns={'SIGNER_Name_x':'SIGNER_Name','SIGNER_Name_y':'Assigned Radiologist','SIGNER_Name':'Assistant'},inplace=True)
## clean out of scope data

phah_combined=phah_combined[~phah_combined['SECTION_CODE'].isin(['CARM','IR'])]
phah_combined=phah_combined[~phah_combined['WORKPLACE_CODE'].isin(['US-IR'])]
phah_combined=phah_combined[~phah_combined['PROCEDURE_STATUS_LONG'].isin(['del'])]
phah_combined=phah_combined[~phah_combined['PROCEDURE_NAME'].str.contains('dental',case=False)]
phah_combined=phah_combined[~phah_combined['PROCEDURE_NAME'].str.contains('-arm',case=False)]
phah_combined=phah_combined[~phah_combined['PROCEDURE_NAME'].str.contains('XR Panorex/Orthopantogram',case=False)]
phah_combined=phah_combined[~phah_combined['PROCEDURE_NAME'].str.contains('CT MYELOGRAM',case=False)]










phah_combined.loc[phah_combined['PROCEDURE_STATUS_LONG']!='appr','PROCEDURE_STATUS']="Exam Ended"

phah_combined.loc[phah_combined['PROCEDURE_STATUS_LONG']=='exam','PROCEDURE_STATUS']="Exam Ended"
phah_combined.loc[phah_combined['PROCEDURE_STATUS_LONG']=='appr','PROCEDURE_STATUS']="Final"
phah_combined.loc[phah_combined['PROCEDURE_STATUS_LONG']=='writ','PROCEDURE_STATUS']="Saved"
phah_combined['PD_MISC_NUMBER_1']=phah_combined['PD_MISC_NUMBER_1'].astype(str).str.split(pat=".").str[0]

phah_combined.loc[phah_combined['PD_MISC_NUMBER_1'].str[0]=='1','PD_MISC_MFD_1_DESCRIPTION']="Saudi"

phah_combined.loc[phah_combined['PD_MISC_NUMBER_1'].str[0]!='1','PD_MISC_MFD_1_DESCRIPTION']="Non Saudi"


phah_combined['Hospital']='PMAH'

phah_combined.info()

#phah_combined.loc[(phah_combined['SECTION_CODE']=='HE'),"PROCEDURE_NAME"]=phah_combined[]

phah_combined.loc[(phah_combined['SECTION_CODE']=='BII')&(phah_combined['PROCEDURE_NAME'].str.contains('Mammogram')),'SECTION_CODE']="Mamo"
phah_combined.loc[(phah_combined['SECTION_CODE']=='BII')&(phah_combined['PROCEDURE_NAME'].str.contains('MA ')),'SECTION_CODE']="Mamo"

#phah_combined.loc[(phah_combined['SECTION_CODE']=='CT')&(phah_combined['PROCEDURE_NAME'].str.contains('Biopsy')),'SECTION_CODE']="IR"
phah_combined.loc[(phah_combined['SECTION_CODE']=='NM')&(phah_combined['PROCEDURE_NAME'].str.contains('BMD ')),'SECTION_CODE']="X-Ray (BMD)"
#phah_combined.loc[(phah_combined['SECTION_CODE'].str.contains('NM'))&(phah_combined['PROCEDURE_NAME'].str.contains('PET')),'SECTION_CODE']="PET-CT"
phah_combined.loc[phah_combined['SECTION_CODE']=='NM','SECTION_CODE']="Other NM"
phah_combined.loc[phah_combined['SECTION_CODE']=='RF','SECTION_CODE']="X-Ray (Fluoro)"# need more about carm in RF
# phah_combined.loc[(phah_combined['SECTION_CODE']=='US')&(phah_combined['PROCEDURE_NAME'].str.contains('Biopsy')),'SECTION_CODE']="IR"
# phah_combined.loc[(phah_combined['SECTION_CODE']=='US')&(phah_combined['PROCEDURE_NAME'].str.contains('FNA')),'SECTION_CODE']="IR"
# phah_combined.loc[(phah_combined['SECTION_CODE']=='US')&(phah_combined['PROCEDURE_NAME'].str.contains('GUIDED')),'SECTION_CODE']="IR"
# phah_combined.loc[(phah_combined['SECTION_CODE']=='US')&(phah_combined['PROCEDURE_NAME'].str.contains(' guiding ')),'SECTION_CODE']="IR"
# phah_combined.loc[(phah_combined['SECTION_CODE']=='US')&(phah_combined['PROCEDURE_NAME'].str.contains('GUIDING ')),'SECTION_CODE']="IR"
phah_combined.loc[(phah_combined['SECTION_CODE']=='XRAY'),'SECTION_CODE']="X-Ray"







# phah_combined['PAT_BIRTH_DATE']=pd.to_datetime(phah_combined['PAT_BIRTH_DATE'],errors='coerce').dt.date

phah_combined['Age']=(datetime.today() - phah_combined['PAT_BIRTH_DATE']).dt.days/365

phah_combined["SIGNER_Name"]=phah_combined["SIGNER_Name"].str.upper().str.strip()
phah_combined["Assigned Radiologist"]=phah_combined["Assigned Radiologist"].str.upper().str.strip()


phah_combined["Assistant"]=phah_combined["Assistant"].str.upper().str.strip()


Radiolgistnames["PMAH"]=Radiolgistnames["PMAH"].str.upper().str.strip()
Radiolgistnames.fillna(0,inplace=True)


phah_combined["SIGNER_Name2"]=phah_combined["SIGNER_Name"].map(Radiolgistnames[Radiolgistnames["PMAH"]!=0].set_index("PMAH")['Final unified list'])
phah_combined["Assigned Radiologist"]=phah_combined["Assigned Radiologist"].map(Radiolgistnames[Radiolgistnames["PMAH"]!=0].set_index("PMAH")['Final unified list'])
phah_combined["Assistant"]=phah_combined["Assistant"].map(Radiolgistnames[Radiolgistnames["PMAH"]!=0].set_index("PMAH")['Final unified list'])

invest=phah_combined.loc[(phah_combined["SIGNER_Name2"]==0)|(phah_combined["SIGNER_Name2"].isnull())]

phah_combined['First Visit or Follow up?']="First Time"



phah_all=phah_combined.drop_duplicates(subset=['PROCEDURE_KEY'], keep='first')


procduremapping_pointspmh=procduremapping_points[['PMAH Examinations Names','NICIP Examination Name']]



phah_all["PROCEDURE_NAME2"]=phah_all["PROCEDURE_NAME"]

phah_all["PROCEDURE_NAME2"]=phah_all["PROCEDURE_NAME2"].str.upper()
phah_all["PROCEDURE_NAME2"]=phah_all["PROCEDURE_NAME2"].apply(lambda x: x.replace(' ','') )
procduremapping_pointspmh['PMAH Examinations Names']=procduremapping_pointspmh['PMAH Examinations Names'].astype(str)
procduremapping_pointspmh['PMAH Examinations Names']=procduremapping_pointspmh['PMAH Examinations Names'].str.upper()
procduremapping_pointspmh['PMAH Examinations Names']=procduremapping_pointspmh['PMAH Examinations Names'].apply(lambda x: x.replace(' ','') )

procduremapping_pointspmh=procduremapping_pointspmh.drop_duplicates(['PMAH Examinations Names'])

phah_all["PROCEDURE_NAME_Nicp"]=phah_all["PROCEDURE_NAME2"].map(procduremapping_pointspmh[procduremapping_pointspmh["PMAH Examinations Names"]!="nan"].set_index("PMAH Examinations Names")['NICIP Examination Name'])
#phah_all["PROCEDURE_NAME_Nphes"]=phah_all["PROCEDURE_NAME"].map(procduremapping_pointspmh[procduremapping_pointspmh["PMAH Examinations Names"]!=""].set_index("PMAH Examinations Names")['NPHIES Examination Name'])

phah_all.loc[phah_all['PROCEDURE_NAME2']=='NAN','PROCEDURE_NAME_Nicp']=" "
phah_all['Anat Region']='Unknown'
#phah_all['Reading Physician']="["+phah_all["READER_CODE"]+']['+phah_all["COREADER_CODE"]+']'
phah_all['Reading Physician']="["+phah_all["DICT_PERS_CODE2"]+']'



phah_all.info()

pmah_notmappedprocedires=phah_all.loc[phah_all['PROCEDURE_NAME_Nicp'].isnull()]['PROCEDURE_NAME'].value_counts().reset_index()

pmah_notmappedradio=phah_all.loc[(phah_all['SIGNER_Name2'].isnull())&(phah_all['PROCEDURE_STATUS']=='Final')]['SIGNER_Name'].value_counts().reset_index()

#phah_all.to_excel(r'D:\AAML\CCC\Hospitals data\PMAH\PMAH contract modalityall.xlsx', sheet_name = "modality", index = False) 

phah_all=phah_all.loc[~phah_all['PROCEDURE_KEY'] .isin( Old_data ['PROCEDURE_KEY'])]



############################KFMC read data#############################

kfmcnew = pd.read_excel(r"D:\AAML\CCC\Hospitals data\KFMC\Imaging_Master_Status_Template__01 Sep 2024 to 16 Nov 2024.xlsx")



############Historical data part 

# kfmcold = pd.read_excel(r"D:\AAML\CCC\Hospitals data\KFMC\MIA_Radiology_Monthly_Statistics_Last_year_results.xlsx")
# kfmc= pd.concat([kfmcold, kfmcnew], ignore_index=True,axis=0)  


kfmc=kfmcnew

kfmc=kfmc.drop_duplicates(subset=['Accession #'], keep='last')

#kfmc.info()









kfmc = kfmc.drop(['Finalized Time.1','Priority.1','IMG END EXAM to Signed','GA','Date Signed','Time Signed','End to Prelim (min)','End to Prelim/Sign (min)','End Exam Charge Triggered'], axis=1)

kfmc['Finalized Time'].rename()
kfmc_dupp=kfmc[kfmc['Accession #'].duplicated(keep=False)==True]
kfmc['contract_modality'] = "Null"
kfmc.loc[kfmc['Category'].str.contains('CV'),'contract_modality']="US"

# kfmc.loc[(kfmc['Category'].str.contains('BI'))&(kfmc['Procedure'].str.contains(' MAMMOGRAM')),'contract_modality']="Mamo"
# kfmc.loc[(kfmc['Category'].str.contains('BI'))&(kfmc['Procedure'].str.contains(' STEREOTACTIC')),'contract_modality']="Mamo"

# kfmc.loc[(kfmc['Category'].str.contains('BI'))&(kfmc['contract_modality']=='Null'),'contract_modality']="US"
kfmc.loc[kfmc['Category'].str.contains('BI'),'contract_modality']="Mamo"

kfmc.loc[(kfmc['Category'].str.contains('IR')),'contract_modality']="IR"

kfmc.loc[(kfmc['Category'].str.contains('IR'))&(kfmc['Modality'].str.contains(' CT ')),'contract_modality']="CT"
kfmc.loc[(kfmc['Category'].str.contains('IR'))&(kfmc['Modality'].str.contains(' Fluoro ')),'contract_modality']="X-Ray (Fluoro)"








kfmc.Modality.value_counts()
kfmc = kfmc[~kfmc.Procedure.str.contains('CONSULTATION',case=False)]
kfmc = kfmc[((kfmc['contract_modality'] != 'IR'))]
kfmc = kfmc[~kfmc['Category'] .str.contains( ' OB ')]

#kfmc.loc[(kfmc['Category'].str.contains('BI'))&(kfmc['contract_modality']=='Null'),'contract_modality']="IR"# remove 
#aa=kfmc.loc[kfmc['Procedure ID']=='IMG87222']
kfmc.loc[kfmc['Category'].str.contains('DXA'),'contract_modality']="X-Ray (BMD)"
kfmc.loc[kfmc['Category'].str.contains('FLUOROSCOPY'),'contract_modality']="X-Ray (Fluoro)"
#kfmc.loc[(kfmc['Category'].str.contains('IR'))&(kfmc['Procedure'].str.contains(' CT ')),'contract_modality']="CT"
kfmc.loc[kfmc['Category'].str.contains('CT'),'contract_modality']="CT"
kfmc.loc[kfmc['Category'].str.contains('MRI'),'contract_modality']="MRI"
kfmc.loc[(kfmc['Category'].str.contains('NM'))&(kfmc['Procedure'].str.contains('PET')),'contract_modality']="PET-CT"
kfmc.loc[(kfmc['Category'].str.contains('NM'))&(kfmc['contract_modality']=='Null'),'contract_modality']="Other NM"
kfmc.loc[kfmc['Category']=='IMG US PROCEDURES','contract_modality']="US"
kfmc.loc[kfmc['Category']=='IMG XR PROCEDURES','contract_modality']="X-Ray"
kfmc=kfmc[~kfmc.Modality.str.contains('arm',case=False,na=False)]
kfmc=kfmc[~kfmc['Signing Physicians'].str.contains('Almashouq, Taghrid',case=False,na=False)]


##############outside

kfmc.loc[(kfmc['Procedure'].str.contains('OUTSIDE',case=False))&(kfmc['Procedure'].str.contains('CT')),'contract_modality']="CT"
kfmc.loc[(kfmc['Procedure'].str.contains('OUTSIDE',case=False))&(kfmc['Procedure'].str.contains('ULTRASOUND')),'contract_modality']="US"
kfmc.loc[(kfmc['Procedure'].str.contains('OUTSIDE',case=False))&(kfmc['Procedure'].str.contains('PET ')),'contract_modality']="PET-CT"
kfmc.loc[(kfmc['Procedure'].str.contains('OUTSIDE',case=False))&(kfmc['Procedure'].str.contains('VOIDING ')),'contract_modality']="X-Ray (Fluoro)"
kfmc.loc[(kfmc['Procedure'].str.contains('OUTSIDE',case=False))&(kfmc['Procedure'].str.contains('BARIUM ')),'contract_modality']="X-Ray (Fluoro)"

kfmc.loc[(kfmc['Procedure'].str.contains('OUTSIDE',case=False))&(kfmc['Procedure'].str.contains('X-RAY')),'contract_modality']="X-Ray"
kfmc.loc[(kfmc['Procedure'].str.contains('OUTSIDE',case=False))&(kfmc['Procedure'].str.contains('NM ')),'contract_modality']="Other NM"
kfmc.loc[(kfmc['Procedure'].str.contains('OUTSIDE',case=False))&(kfmc['Procedure'].str.contains('BONE SCAN')),'contract_modality']="Other NM"
kfmc.loc[(kfmc['Procedure'].str.contains('OUTSIDE',case=False))&(kfmc['Procedure'].str.contains('NUCLEAR')),'contract_modality']="Other NM"
kfmc.loc[(kfmc['Procedure'].str.contains('OUTSIDE',case=False))&(kfmc['Procedure'].str.contains('MAMMOGRAM')),'contract_modality']="Mamo"
kfmc.loc[(kfmc['Procedure'].str.contains('OUTSIDE',case=False))&(kfmc['Procedure'].str.contains('MRI ', case=False)),'contract_modality']="MRI"


kfmc=kfmc[~kfmc['Procedure'].str.contains('ANGIOGRAM DONE OUTSIDE')]



# Radiolgistnames.Category.value_counts()
# ####################Assistant radioloist 
# kfmc1=kfmc.copy()
# assistantList=Radiolgistnames.loc[Radiolgistnames['Category']=='Assistant ']

# # Split the strings in the 'fruits' column and create a new column with the split elements
# kfmc1['Reading Physician'] = kfmc1['Reading Physician'].str.split('\n')

# # Apply a condition to select one element
# selected_fruit = kfmc1.apply(lambda row: [assis for assis in row['Reading Physician'] if assis in assistantList['KFMC']][0])    

# kfmc1['selected_fruit'] = kfmc1.apply(lambda row: next((assist for assist in row['Reading Physician'] if assist in assistantList['KFMC']), None), axis=1)
# #kfmc.to_excel(r'D:\AAML\CCC\Hospitals data\KFMC contract modality.xlsx', sheet_name = "modality", index = False) 


kfmc['order_datetime']= kfmc['Order Date'].astype(str) + ' ' + kfmc['Order Time'].astype(str)
kfmc['Accession #']= 'KFMC' + '_' + kfmc['Accession #'].astype(str)
kfmc['MRN']= 'KFMC' + '_' + kfmc['MRN'].astype(str)
kfmc.loc[kfmc['Sex']=='F','Sex']="W"

kfmc['Nationality']=kfmc['Nationality'].astype(str)



kfmc.loc[kfmc['Base Pt Class']=='ED','Base Pt Class']="E"
kfmc.loc[kfmc['Base Pt Class']=='OP','Base Pt Class']="O"
kfmc.loc[kfmc['Base Pt Class']=='IP','Base Pt Class']="I"
kfmc['SSN']=kfmc['SSN'].astype(str).str.split(pat=".").str[0]
kfmc.loc[kfmc['SSN'].str[0]=='1','Nationality']="Saudi"

kfmc.loc[kfmc['SSN'].str[0]!='1','Nationality']="Non Saudi"

kfmc['Age']=(datetime.today() - kfmc['Birth Date']).dt.days/365


kfmc['Hospital']='KFMC'


# Protocol info spilit
# ctabdome=kfmc[kfmc['Procedure'].str.contains('CT ABDOMEN')]
# kfmcprotocol=pd.DataFrame(ctabdome['Protocol Info'])
# kfmcprotocol=kfmcprotocol['Protocol Info'].str.split('\n',expand=True)

                               






withoutprelimkfmc=kfmc[kfmc['Prelimm Instant UTC'].isnull()]




kfmcrenamed = kfmc.rename(columns=mapping.set_index('KFMC')['PMAH'].to_dict())


procduremapping_pointskfmc=procduremapping_points
kfmcrenamed["PROCEDURE_NAME2"]=kfmcrenamed["PROCEDURE_NAME"]

kfmcrenamed["PROCEDURE_NAME2"]=kfmcrenamed["PROCEDURE_NAME2"].str.upper()
kfmcrenamed["PROCEDURE_NAME2"]=kfmcrenamed["PROCEDURE_NAME2"].apply(lambda x: x.replace(' ','') )
procduremapping_pointskfmc['KFMC Examination Name']=procduremapping_pointskfmc['KFMC Examination Name'].astype(str)
procduremapping_pointskfmc['KFMC Examination Name']=procduremapping_pointskfmc['KFMC Examination Name'].str.upper()
procduremapping_pointskfmc['KFMC Examination Name']=procduremapping_pointskfmc['KFMC Examination Name'].apply(lambda x: x.replace(' ','') )

procduremapping_pointskfmc=procduremapping_pointskfmc.drop_duplicates(['KFMC Examination Name'])

kfmcrenamed["PROCEDURE_NAME_Nicp"]=kfmcrenamed["PROCEDURE_NAME2"].map(procduremapping_pointskfmc[procduremapping_pointskfmc['KFMC Examination Name']!="nan"].set_index('KFMC Examination Name')['NICIP Examination Name'])
#phah_all["PROCEDURE_NAME_Nphes"]=phah_all["PROCEDURE_NAME"].map(procduremapping_pointspmh[procduremapping_pointspmh["PMAH Examinations Names"]!=""].set_index("PMAH Examinations Names")['NPHIES Examination Name'])




kfmcrenamed.loc[kfmcrenamed['PROCEDURE_NAME2']=='NAN','PROCEDURE_NAME_Nicp']=" "

kfmcrenamed["SIGNER_Name"]=kfmcrenamed["SIGNER_Name"].str.upper().str.strip()
kfmcrenamed["Assigned Radiologist"]=kfmcrenamed["Assigned Radiologist"].str.upper().str.strip()


kfmcrenamed["Assistant"]=kfmcrenamed["Assistant"].str.upper().str.strip()


Radiolgistnames["KFMC2"]=Radiolgistnames["KFMC"].str.split('[').str[0].str.upper().str.strip()

Radiolgistnames.fillna(0,inplace=True)
kfmcrenamed["SIGNER_Name2"]=kfmcrenamed["SIGNER_Name"].map(Radiolgistnames[Radiolgistnames["KFMC2"]!=0].set_index("KFMC2")['Final unified list'])
kfmcrenamed["Assigned Radiologist"]=kfmcrenamed["Assigned Radiologist"].str.split('[').str[0].str.upper().str.strip().map(Radiolgistnames[Radiolgistnames["KFMC2"]!=0].set_index("KFMC2")['Final unified list'])
kfmcrenamed["Assistant"]=kfmcrenamed["Assistant"].map(Radiolgistnames[Radiolgistnames["KFMC2"]!=0].set_index("KFMC2")['Final unified list'])

kfmcnew["Assistant"].value_counts()

kfmcrenamed["Assistant"].value_counts()
#kfmcrenamed["SIGNER_Name2"]=kfmcrenamed["SIGNER_Name"]
kfmcrenamed.columns[50:70]
aaxx=kfmcrenamed[['Assigned Radiologist','Assigned Physicians', 'Assigned Owners']]


phah_combined.reset_index(inplace=True, drop=True)
kfmcrenamed.reset_index(inplace=True, drop=True)

kfmcrenamed.info()

#kfmcrenamed_withouassigned=kfmcrenamed.loc[kfmcrenamed["Assigned Radiologist2"].isnull()][['PROCEDURE_KEY','PROCEDURE_NAME','PROCEDURE_STATUS','PROCEDURE_END','Assigned Radiologist','Assigned Radiologist2','Assigned Physicians', 'Assigned Owners']]
KFMC_notmappedprocedires=kfmcrenamed.loc[kfmcrenamed['PROCEDURE_NAME_Nicp'].isnull()]['PROCEDURE_NAME'].value_counts().reset_index()

phah_kfmc_combined= pd.concat([phah_all, kfmcrenamed], ignore_index=True,axis=0)  

#phah_kfmc_combined.to_excel(r'D:\AAML\CCC\Hospitals data\kfmc procedure'+datetime.today().strftime("%d %b, %Y")+'.xlsx', sheet_name = "All", index = False) 
#invest=kfmcrenamed.loc[((kfmcrenamed["SIGNER_Name2"]=='Dr. Feras Essa Alomar'))]

kfmc_notmappedradio=kfmcrenamed.loc[(kfmcrenamed['SIGNER_Name2'].isnull())&(kfmcrenamed['PROCEDURE_STATUS']=='Final')]['SIGNER_Name'].value_counts().reset_index()

# Alia=kfmcrenamed.loc[(kfmcrenamed['SIGNER_Name']=='ALIYA IBRAHIM ALAWAJI')]

# Outsid final and there is no signer name 

phah_kfmc_combined.info(50)


################################# ِAl Yamamh###################
####modality to sction group, admission type 

alyma_rawperformed = pd.read_excel(r"D:\AAML\CCC\Hospitals data\AlYamamh\C2 01 Sep 2024 to 16 Nov 2024 Exam.xlsx")
alyma_rawreported = pd.read_excel(r"D:\AAML\CCC\Hospitals data\AlYamamh\C2 01 Sep 2024 to 16 Nov 2024 Reported.xlsx")
alyma_rawperformed.info()

###  in performed but not in reported 

alyma_rawperformed_w_duplic=alyma_rawperformed.loc[~alyma_rawperformed['PROCEDURE_KEY'] .isin( alyma_rawreported['PROCEDURE_KEY'])]

alyma_combined=pd.concat([alyma_rawreported, alyma_rawperformed_w_duplic], ignore_index=True)

alyma_combined=alyma_combined.loc[alyma_combined['PAT_FIRST_NAME']!='Test']

alyma_combined=alyma_combined.loc[alyma_combined['PROCEDURE_STATUS_LONG']!='del']


# phah_combined['PP_MISC_TEXT_22']=phah_combined['PP_MISC_TEXT_2'].astype(str).str.split(pat=".").str[0]

# phah_combined['PP_MISC_TEXT_23']=pd.to_datetime(phah_combined['PP_MISC_TEXT_22'].str[:4]   +"-"+phah_combined['PP_MISC_TEXT_22'].str[4:6]  +"-"+   phah_combined['PP_MISC_TEXT_22'].str[6:8] +" "+   phah_combined['PP_MISC_TEXT_22'].str[8:10] +":"+   phah_combined['PP_MISC_TEXT_22'].str[10:12]+":"+   phah_combined['PP_MISC_TEXT_22'].str[-2:],errors='coerce')


alyma_combined=pd.merge(alyma_combined,alyma_stafff,left_on='SIGNER_CODE',right_on='STAFF_MEMB_CODE',how='left')

# To uncomment once the data for Assigned radiologist and coread is available
alyma_combined=pd.merge(alyma_combined,alyma_stafff,left_on='PERFORMING_DOCTOR',right_on='STAFF_MEMB_CODE',how='left')
#alyma_combined=pd.merge(alyma_combined,alyma_stafff,left_on='COREADER_CODE',right_on='STAFF_MEMB_CODE',how='left')


alyma_combined.rename(columns={'SIGNER_Name_x':'SIGNER_Name','SIGNER_Name_y':'Assigned Radiologist'},inplace=True)
#alyma_combined.rename(columns={'SIGNER_Name_x':'SIGNER_Name','SIGNER_Name_y':'Assigned Radiologist','SIGNER_Name':'Assistant'},inplace=True)

alyma_combined["Assigned Radiologist"]=alyma_combined["Assigned Radiologist"].str.upper().str.strip()


# phah_combined["Assistant"]=phah_combined["Assistant"].str.upper().str.strip()




## clean out of scope data

# phah_combined=phah_combined[~phah_combined['SECTION_CODE'].isin(['CARM','IR'])]
# phah_combined=phah_combined[~phah_combined['WORKPLACE_CODE'].isin(['US-IR'])]
# phah_combined=phah_combined[~phah_combined['PROCEDURE_STATUS_LONG'].isin(['del'])]
# phah_combined=phah_combined[~phah_combined['PROCEDURE_NAME'].str.contains('dental',case=False)]





#alyma_combined['PD_MISC_NUMBER_1'].value_counts()


# alyma_combined.loc[alyma_combined['PROCEDURE_STATUS_LONG']!='appr','PROCEDURE_STATUS']="Exam Ended"

alyma_combined.loc[alyma_combined['PROCEDURE_STATUS_LONG']=='exam','PROCEDURE_STATUS']="Exam Ended"
alyma_combined.loc[alyma_combined['PROCEDURE_STATUS_LONG']=='appr','PROCEDURE_STATUS']="Final"
alyma_combined.loc[alyma_combined['PROCEDURE_STATUS_LONG']=='writ','PROCEDURE_STATUS']="Saved"
#phah_combined['PD_MISC_NUMBER_1']=phah_combined['PD_MISC_NUMBER_1'].astype(str).str.split(pat=".").str[0]

alyma_combined.loc[alyma_combined['PD_MISC_TEXT_1'].contains('SA'),'PD_MISC_MFD_1_DESCRIPTION']="Saudi"

alyma_combined.loc[~alyma_combined['PD_MISC_TEXT_1'].contains('SA'),'PD_MISC_MFD_1_DESCRIPTION']="Non Saudi"


#alyma_combined['Hospital']='Al Yamamah'
alyma_combined['PAT_NAME']=alyma_combined['PAT_FIRST_NAME']+" "+alyma_combined['PAT_LAST_NAME']



alyma_combined.loc[(alyma_combined['SECTION_CODE'].str.contains('MG')),'SECTION_CODE']="Mamo"
alyma_combined.loc[alyma_combined['SECTION_CODE'].str.contains('CT'),'SECTION_CODE']="CT" 
alyma_combined.loc[alyma_combined['SECTION_CODE'].str.contains('US'),'SECTION_CODE']="US" 
alyma_combined.loc[alyma_combined['SECTION_CODE'].str.contains('RF'),'SECTION_CODE']="X-Ray (Fluoro)"
alyma_combined.loc[alyma_combined['SECTION_CODE'].str.contains('MR'),'SECTION_CODE']="MRI"

alyma_combined.loc[(alyma_combined['SECTION_CODE'].str.contains('XR')),'SECTION_CODE']="X-Ray"
alyma_combined.loc[(alyma_combined['SECTION_CODE'].str.contains('BMD')),'SECTION_CODE']="X-Ray (BMD)"


alyma_combined.loc[(alyma_combined['ADMISSION_TYPE']=='A'),'ADMISSION_TYPE']="O"
alyma_combined.loc[(alyma_combined['ADMISSION_TYPE'].isin(['S','P','V'])),'ADMISSION_TYPE']="I"



alyma_combined['Age']=(datetime.today() - alyma_combined['PAT_BIRTH_DATE']).dt.days/365







alyma_combined["SIGNER_Name"]=alyma_combined["SIGNER_Name"].str.upper().str.strip()
alyma_combined["Assigned Radiologist"]=alyma_combined["Assigned Radiologist"].str.upper().str.strip()




Radiolgistnames["Yamamah"]=Radiolgistnames["Yamamah"].str.upper().str.strip()
Radiolgistnames.fillna(0,inplace=True)


alyma_combined["SIGNER_Name2"]=alyma_combined["SIGNER_Name"].map(Radiolgistnames[Radiolgistnames["Yamamah"]!=0].set_index("Yamamah")['Final unified list'])
alyma_combined["Assigned Radiologist"]=alyma_combined["Assigned Radiologist"].map(Radiolgistnames[Radiolgistnames["Yamamah"]!=0].set_index("Yamamah")['Final unified list'])
# phah_combined["Assistant"]=phah_combined["Assistant"].map(Radiolgistnames[Radiolgistnames["PMAH"]!=0].set_index("PMAH")['Final unified list'])

invest=alyma_combined.loc[(alyma_combined["SIGNER_Name2"]==0)|(alyma_combined["SIGNER_Name2"].isnull())]

alyma_combined['First Visit or Follow up?']="First Time"



alyma_combined2=alyma_combined.drop_duplicates(subset=['PROCEDURE_KEY'], keep='first')


procduremapping_pointsdwadme=procduremapping_points[['NPHIES Examination Name','NICIP Examination Name']]
procduremapping_pointsdwadme.fillna(0,inplace=True)
procduremapping_pointsdwadme=procduremapping_pointsdwadme.loc[(procduremapping_pointsdwadme['NPHIES Examination Name']!=0)&(procduremapping_pointsdwadme['NICIP Examination Name']!=0)&(procduremapping_pointsdwadme['NPHIES Examination Name']!=" ")&(procduremapping_pointsdwadme['NPHIES Examination Name']!="  ")&(procduremapping_pointsdwadme['NPHIES Examination Name'].str.strip()!=" ")]
procduremapping_pointsdwadme['NPHIES Examination Name']=procduremapping_pointsdwadme['NPHIES Examination Name'].astype(str)
procduremapping_pointsdwadme['NPHIES Examination Name']=procduremapping_pointsdwadme['NPHIES Examination Name'].str.upper()
procduremapping_pointsdwadme['NPHIES Examination Name']=procduremapping_pointsdwadme['NPHIES Examination Name'].apply(lambda x: x.replace(' ','') )
procduremapping_pointsdwadme=procduremapping_pointsdwadme.drop_duplicates(['NPHIES Examination Name'])



alyma_combined2["PROCEDURE_NAME2"]=alyma_combined2["PROCEDURE_NAME"]

alyma_combined2["PROCEDURE_NAME2"]=alyma_combined2["PROCEDURE_NAME2"].astype(str).str.upper()
alyma_combined2["PROCEDURE_NAME2"]=alyma_combined2["PROCEDURE_NAME2"].apply(lambda x: x.replace(' ','') )

alyma_combined2["PROCEDURE_NAME_Nicp"]=alyma_combined2["PROCEDURE_NAME2"].map(procduremapping_pointsdwadme[procduremapping_pointsdwadme['NPHIES Examination Name']!="nan"].set_index('NPHIES Examination Name')['NICIP Examination Name'])

alyma_combined2.loc[alyma_combined2['PROCEDURE_NAME2']=='NAN','PROCEDURE_NAME_Nicp']=" "



alyma_combined2_notmappedprocedires=alyma_combined2.loc[alyma_combined2['PROCEDURE_NAME_Nicp'].isnull()]['PROCEDURE_NAME'].value_counts().reset_index()

alyma_combined2_notmappedradio=alyma_combined2.loc[(alyma_combined2['SIGNER_Name2'].isnull())&(alyma_combined2['PROCEDURE_STATUS']=='Final')]['SIGNER_Name'].value_counts().reset_index()

alyma_combined2.loc[alyma_combined2['WORKPLACE_CODE'].str.contains('YAM'),'Hospital']="Al Yamamah"
alyma_combined2.loc[alyma_combined2['WORKPLACE_CODE'].str.contains('MAJ'),'Hospital']='Al Majmaah'
alyma_combined2.loc[alyma_combined2['WORKPLACE_CODE'].str.contains('ZUF'),'Hospital']="Al Zulfi"
alyma_combined2.loc[alyma_combined2['WORKPLACE_CODE'].str.contains('DW'),'Hospital']='Al Dawadmi'
alyma_combined2.loc[alyma_combined2['WORKPLACE_CODE'].str.contains('ART'),'Hospital']='Al Artaweyyah'


Almajmah_new=alyma_combined2.loc[alyma_combined2['Hospital']=='Al Majmaah']
AlZulfi_new=alyma_combined2.loc[alyma_combined2['Hospital']=='Al Zulfi']
Dwadme_new=alyma_combined2.loc[alyma_combined2['Hospital']=='Al Dawadmi']
Artwyea_new=alyma_combined2.loc[alyma_combined2['Hospital']=='Al Artaweyyah']


alyma_combined2=alyma_combined2.loc[alyma_combined2['Hospital']=="Al Yamamah"]
alyma_combined2['PROCEDURE_KEY']= 'ALYMAMH' + '_' + alyma_combined2['PROCEDURE_KEY'].astype(str)
alyma_combined2['PD_MISC_NUMBER_1']= 'ALYMAMH' + '_' + alyma_combined2['PD_MISC_NUMBER_1'].astype(str)

AlZulfi_new['PROCEDURE_KEY']= "Al Zulfi" + '_' + AlZulfi_new['PROCEDURE_KEY'].astype(str)
AlZulfi_new['PD_MISC_NUMBER_1']= "Al Zulfi" + '_' + AlZulfi_new['PD_MISC_NUMBER_1'].astype(str)



Almajmah_new['PROCEDURE_KEY']= 'Al Majmaah' + '_' + Almajmah_new['PROCEDURE_KEY'].astype(str)
Almajmah_new['PD_MISC_NUMBER_1']= 'Al Majmaah'+ '_' + Almajmah_new['PD_MISC_NUMBER_1'].astype(str)


Dwadme_new['PROCEDURE_KEY']= 'Al Dawadmi' + '_' + Dwadme_new['PROCEDURE_KEY'].astype(str)
Dwadme_new['PD_MISC_NUMBER_1']= 'Al Dawadmi'+ '_' + Dwadme_new['PD_MISC_NUMBER_1'].astype(str)


Artwyea_new['PROCEDURE_KEY']= 'Al Artaweyyah' + '_' + Artwyea_new['PROCEDURE_KEY'].astype(str)
Artwyea_new['PD_MISC_NUMBER_1']= 'Al Artaweyyah'+ '_' + Artwyea_new['PD_MISC_NUMBER_1'].astype(str)



#phah_all.to_excel(r'D:\AAML\CCC\Hospitals data\PMAH\PMAH contract modalityall.xlsx', sheet_name = "modality", index = False) 

old_alyma = pd.read_excel(r"D:\AAML\CCC\Hospitals data\AlYamamh\Alyamamh_OldRis_06 Oct, 2024.xlsx")

alyma_all= pd.concat([alyma_combined2, old_alyma], ignore_index=True,axis=0)  
ax=alyma_all['PROCEDURE_KEY'].duplicated()














phah_kfmc_yamamh= pd.concat([phah_kfmc_combined, alyma_all], ignore_index=True,axis=0)  

#phah_kfmc_yamamh.to_excel(r'D:\AAML\CCC\Hospitals data\phah_kfmc_ymamh'+datetime.today().strftime("%d %b, %Y")+'.xlsx', sheet_name = "All", index = False) 


################################# ِAl Yamamh###################
####modality to sction group, admission type 


alartwiah = pd.read_excel(r"D:\AAML\CCC\Hospitals data\AlArtwiah\Artawiah - Dec. 1st to Oct. 31st.xlsx")

alartwiah.info()

alartwiah=alartwiah[alartwiah['Status']!='Cancel']


# mapping admission type 
alartwiah.loc[alartwiah['Custom Field 1']=='Emergency','Type']="E"
alartwiah.loc[alartwiah['Custom Field 1']=='OutPatient','Type']="O"
alartwiah.loc[alartwiah['Custom Field 1']=='InPatient','Type']="I"
alartwiah.loc[alartwiah['Custom Field 1']=='Inpatient Daycase','Type']="I"

# mapping modalitiy with contract
alartwiah['contract_modality']=alartwiah['Mod.']
alartwiah.loc[alartwiah['Mod.']=='DX','contract_modality']="X-Ray"
alartwiah.loc[alartwiah['Mod.']=='CR','contract_modality']="X-Ray"
alartwiah.loc[alartwiah['Mod.']=='CT','contract_modality']="CT"
# Nationality
# alartwiah.loc[alartwiah['Nationality']!='SAU','Nationality']="Non Saudi"
 
# alartwiah.loc[alartwiah['Nationality']=='SAU','Nationality']="Saudi"

#alartwiah['contract_modality']=alartwiah['Study group']
alartwiah['Mod.'].value_counts()
alartwiah['Description2']=alartwiah['Description']


alartwiah.loc[alartwiah['Patient ID'].str[0]=='1','Nationality']="Saudi"

alartwiah.loc[alartwiah['Patient ID'].str[0]!='1','Nationality']="Non Saudi"


alartwiah.loc[~alartwiah['Status'].str.contains('Final'),'Status']="Exam Ended"
alartwiah.loc[alartwiah['Status'].str.contains('Final'),'Status']="Final"


alartwiah['Scan Datetime']= pd.to_datetime(alartwiah['Date'].astype(str) + ' ' + alartwiah['Time'].astype(str))
#alartwiah['Scan Datetime']=pd.to_datetime(alartwiah['Scan Datetime'],dayfirst=True)

alartwiah['Accession']= 'Artaweyyah' + '_' + alartwiah['Accession'].astype(str)
alartwiah['ID']= 'Artaweyyah' + '_' + alartwiah['Patient ID'].astype(str)

alartwiah['Age']=(datetime.today() - alartwiah['Birth Date']).dt.days/365

alartwiahrenamed = alartwiah.rename(columns=mapping.set_index('AlArtwiah')['PMAH'].to_dict())

alartwiahrenamed = alartwiahrenamed[alartwiahrenamed.columns[alartwiahrenamed.columns.isin (mapping['PMAH'])]]



alartwiahrenamed["SIGNER_Name"]=alartwiahrenamed["SIGNER_Name"].str.upper().str.strip()

alartwiahrenamed["Assigned Radiologist"]=alartwiahrenamed["Assigned Radiologist"].str.upper().str.strip()


Radiolgistnames["Artawiyyah"]=Radiolgistnames["Artawiyyah"].str.upper().str.strip()
Radiolgistnames.fillna(0,inplace=True)




alartwiahrenamed["SIGNER_Name2"]=alartwiahrenamed["SIGNER_Name"].map(Radiolgistnames[Radiolgistnames["Artawiyyah"]!=0].set_index("Artawiyyah")['Final unified list'])

alartwiahrenamed["Assigned Radiologist"]=alartwiahrenamed["Assigned Radiologist"].map(Radiolgistnames[Radiolgistnames["Artawiyyah"]!=0].set_index("Artawiyyah")['Final unified list'])



alartwiahrenamed=alartwiahrenamed.drop_duplicates(['PROCEDURE_KEY'])


#alartwiahrenamed['REPORT_VERIFICATION_DATE']=pd.to_datetime(alartwiahrenamed['REPORT_VERIFICATION_DATE'],format='%m/%d/%Y %I:%M:%S %p')
alartwiahrenamed['REPORT_VERIFICATION_DATE']=pd.to_datetime(alartwiahrenamed['REPORT_VERIFICATION_DATE'])




alartwiahrenamed["PROCEDURE_NAME2"]=alartwiahrenamed["PROCEDURE_NAME"]

alartwiahrenamed["PROCEDURE_NAME2"]=alartwiahrenamed["PROCEDURE_NAME2"].astype(str).str.upper()
alartwiahrenamed["PROCEDURE_NAME2"]=alartwiahrenamed["PROCEDURE_NAME2"].apply(lambda x: x.replace(' ','') )


alartwiahrenamed["PROCEDURE_NAME_Nicp"]=alartwiahrenamed["PROCEDURE_NAME2"].map(procduremapping_pointsdwadme[procduremapping_pointsdwadme['NPHIES Examination Name']!="nan"].set_index('NPHIES Examination Name')['NICIP Examination Name'])

alartwiahrenamed.loc[alartwiahrenamed['PROCEDURE_NAME2']=='NAN','PROCEDURE_NAME_Nicp']=" "






alartwiahrenamed['Hospital']='Al Artaweyyah'
alartwiahrenamed2=pd.concat([alartwiahrenamed, Artwyea_new], ignore_index=True,axis=0)  


phah_kfmc_yamamh_ar= pd.concat([phah_kfmc_yamamh, alartwiahrenamed2], ignore_index=True,axis=0)  

Alartawyah_notmappedradio=alartwiahrenamed.loc[(alartwiahrenamed['SIGNER_Name2'].isnull())&(alartwiahrenamed['PROCEDURE_STATUS']=='Final')]['SIGNER_Name'].value_counts().reset_index()


#phah_kfmc_yamamh_ar.to_excel(r'D:\AAML\CCC\Hospitals data\phah_kfmc_yamamh_ar'+datetime.today().strftime("%d %b, %Y")+'.xlsx', sheet_name = "All", index = False) 
###########################################################################

##### report status to study status 


dwadme = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Dwadme\DAWADMI 1 SEP - 16 NOV.xlsx")
dwadme.info()

dwadme['Patient ID']=dwadme['Patient ID'].astype(str).str.split(pat=".").str[0]

dwadme.loc[dwadme['Patient ID'].str[0]=='1','Nationality']="Saudi"

dwadme.loc[dwadme['Patient ID'].str[0]!='1','Nationality']="Non Saudi"

dwadme=dwadme.drop_duplicates(['Accession number'],keep="last")



dwadme.loc[dwadme['Modality type']=='MG','Modality type']="Mamo"
dwadme.loc[dwadme['Modality type']=='MR','Modality type']="MRI"
dwadme.loc[dwadme['Modality type']=='DX','Modality type']="X-Ray"
dwadme.loc[dwadme['Modality type'].str.contains('RF'),'Modality type']="X-Ray (Fluoro)"
dwadme.loc[dwadme['Modality type']=='BMD','Modality type']="X-Ray (BMD)"
dwadme.loc[dwadme['Modality type'].str.contains('CT'),'Modality type']="CT"
dwadme.loc[dwadme['Modality type'].str.contains('CR'),'Modality type']="X-Ray"

dwadme['Procedure name2']=dwadme['Procedure name']



dwadme.loc[dwadme['Report available']!='Report available','Report status']="Exam Ended"
dwadme.loc[dwadme['Report status']=='Validated','Report status']="Final"
dwadme.loc[dwadme['Report status']=='Preliminary','Report status']="Prelim"

dwadme['Accession number']= 'AlDawadmi' + '_' + dwadme['Accession number'].astype(str)
#dwadme['Patient ID']= 'AlDawadmi' + '_' + dwadme['Patient ID'].astype(str)
dwadme['Scheduled procedure date']= dwadme['Scheduled procedure date'].astype(str) + ' ' + dwadme['Scheduled procedure time'].astype(str)

dwadme['Age']=(datetime.today() - pd.to_datetime(dwadme['Patient date of birth'])).dt.days/365


dawadmerenamed = dwadme.rename(columns=mapping.set_index('AlDwadme')['PMAH'].to_dict())

dawadmerenamed = dawadmerenamed[dawadmerenamed.columns[dawadmerenamed.columns.isin (mapping['PMAH'])]]



dawadmerenamed["SIGNER_Name"]=dawadmerenamed["SIGNER_Name"].str.upper().str.strip()
dawadmerenamed.loc[dawadmerenamed['Assigned Radiologist'].isnull(),'Assigned Radiologist']=dawadmerenamed['Assigned Physicians']


dawadmerenamed["Assigned Radiologist"]=dawadmerenamed["Assigned Radiologist"].str.upper().str.strip()


Radiolgistnames["Dawadmi"]=Radiolgistnames["Dawadmi"].str.upper().str.strip()
Radiolgistnames.fillna(0,inplace=True)


#Radiolgistnames.drop_duplicates(subset=['Dawadmi'],inplace=True)

dawadmerenamed["SIGNER_Name2"]=dawadmerenamed["SIGNER_Name"].map(Radiolgistnames[Radiolgistnames["Dawadmi"]!=0].set_index("Dawadmi")['Final unified list'])
dawadmerenamed["Assigned Radiologist"]=dawadmerenamed["Assigned Radiologist"].map(Radiolgistnames[Radiolgistnames["Dawadmi"]!=0].set_index("Dawadmi")['Final unified list'])
dawadmerenamed.info()
# dawadmerenamed['REQUEST_DATE']=pd.to_datetime(dawadmerenamed['REQUEST_DATE'])
# dawadmerenamed['REPORT_VERIFICATION_DATE']=pd.to_datetime(dawadmerenamed['REPORT_VERIFICATION_DATE'])

# dawadmerenamed['APPOINTMENT_DATE']=pd.to_datetime(dawadmerenamed['APPOINTMENT_DATE'],errors="coerce")
dawadmerenamed['PROCEDURE_END'] = dawadmerenamed['PROCEDURE_END'].apply(datetime.strptime, args=('%d-%b-%Y, %I:%M %p',))


dawadmerenamed['REPORT_VERIFICATION_DATE']=pd.to_datetime(dawadmerenamed['REPORT_VERIFICATION_DATE'],format='%d-%b-%Y, %I:%M %p')



dawadmerenamed["PROCEDURE_NAME2"]=dawadmerenamed["PROCEDURE_NAME"]

dawadmerenamed["PROCEDURE_NAME2"]=dawadmerenamed["PROCEDURE_NAME2"].astype(str).str.upper()
dawadmerenamed["PROCEDURE_NAME2"]=dawadmerenamed["PROCEDURE_NAME2"].apply(lambda x: x.replace(' ','') )


dawadmerenamed["PROCEDURE_NAME_Nicp"]=dawadmerenamed["PROCEDURE_NAME2"].map(procduremapping_pointsdwadme[procduremapping_pointsdwadme['NPHIES Examination Name']!="nan"].set_index('NPHIES Examination Name')['NICIP Examination Name'])

dawadmerenamed.loc[dawadmerenamed['PROCEDURE_NAME2']=='NAN','PROCEDURE_NAME_Nicp']=" "

invtest2=dwadme.loc[dwadme['Accession number']=="AlDawadmi_PRCA000000193299"]




dawadmerenamed['Hospital']='Al Dawadmi'
dawadmerenamed2=pd.concat([dawadmerenamed, Dwadme_new], ignore_index=True,axis=0)  

#dawadmerenamed.to_excel(r"D:\AAML\CCC\Hospitals data\Dwadme\DAWADMI 1DEC-31JAN Ta.xlsx")

ph_kf_yam_ar_dw= pd.concat([phah_kfmc_yamamh_ar, dawadmerenamed2], ignore_index=True,axis=0)  


dwadme_notmappedradio=dawadmerenamed.loc[(dawadmerenamed['SIGNER_Name2'].isnull())&(dawadmerenamed['PROCEDURE_STATUS']=='Final')]['SIGNER_Name'].value_counts().reset_index()


###########################################################################

##### report status to study status 


zulfi = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Zulfi\ZULFI 1 SEP - 16 NOV.xlsx")
zulfi.info()
zulfi=zulfi.drop_duplicates(['Accession number'],keep="last")

zulfi['Social security number']=zulfi['Social security number'].astype(str).str.split(pat=".").str[0]

zulfi.loc[zulfi['Social security number'].str[0]=='1','Nationality']="Saudi"

zulfi.loc[zulfi['Social security number'].str[0]!='1','Nationality']="Non Saudi"


zulfi.loc[zulfi['Modality type']=='MG','Modality type']="Mamo"
zulfi.loc[zulfi['Modality type']=='MR','Modality type']="MRI"
zulfi.loc[zulfi['Modality type'].str.contains('DX'),'Modality type']="X-Ray"
zulfi.loc[zulfi['Modality type']=='CR','Modality type']="X-Ray"

zulfi.loc[zulfi['Modality type'].str.contains('RF'),'Modality type']="X-Ray (Fluoro)"
zulfi.loc[zulfi['Procedure name']=='Hysterosalpingogram','Modality type']="X-Ray (Fluoro)"

zulfi.loc[zulfi['Modality type']=='BMD','Modality type']="X-Ray (BMD)"
zulfi['Procedure name2']=zulfi['Procedure name']


zulfi.loc[zulfi['Report available']!='Report available','Report status']="Exam Ended"
zulfi.loc[zulfi['Report status']=='Validated','Report status']="Final"
zulfi.loc[zulfi['Report status']=='Preliminary','Report status']="Prelim"


zulfi['Accession number']= 'Alzulfi' + '_' + zulfi['Accession number'].astype(str)
zulfi['Patient ID']= 'Alzulfi' + '_' + zulfi['Patient ID'].astype(str)
zulfi['Scheduled procedure date']= zulfi['Scheduled procedure date'].astype(str) + ' ' + zulfi['Scheduled procedure time'].astype(str)

zulfi['Age']=(datetime.today() - pd.to_datetime(zulfi['Patient date of birth'])).dt.days/365


zulfirenamed = zulfi.rename(columns=mapping.set_index('AlZulfi')['PMAH'].to_dict())

zulfirenamed = zulfirenamed[zulfirenamed.columns[zulfirenamed.columns.isin (mapping['PMAH'])]]





zulfirenamed["SIGNER_Name"]=zulfirenamed["SIGNER_Name"].str.upper().str.strip()
zulfirenamed.loc[zulfirenamed['Assigned Radiologist'].isnull(),'Assigned Radiologist']=zulfirenamed['Assigned Physicians']


zulfirenamed["Assigned Radiologist"]=zulfirenamed["Assigned Radiologist"].str.upper().str.strip()


Radiolgistnames["Zulfi"]=Radiolgistnames["Zulfi"].str.upper().str.strip()
Radiolgistnames.fillna(0,inplace=True)



zulfirenamed["SIGNER_Name2"]=zulfirenamed["SIGNER_Name"].map(Radiolgistnames[Radiolgistnames["Zulfi"]!=0].set_index("Zulfi")['Final unified list'])
zulfirenamed["Assigned Radiologist"]=zulfirenamed["Assigned Radiologist"].map(Radiolgistnames[Radiolgistnames["Zulfi"]!=0].set_index("Zulfi")['Final unified list'])

zulfirenamed['PROCEDURE_END'] = zulfirenamed['PROCEDURE_END'].apply(datetime.strptime, args=('%d-%b-%Y, %I:%M %p',))


#zulfirenamed['PROCEDURE_END']=pd.to_datetime(zulfirenamed['PROCEDURE_END'],errors="coerce")
zulfirenamed['REPORT_VERIFICATION_DATE']=pd.to_datetime(zulfirenamed['REPORT_VERIFICATION_DATE'],format='%d-%b-%Y, %I:%M %p')



zulfirenamed["PROCEDURE_NAME2"]=zulfirenamed["PROCEDURE_NAME"]

zulfirenamed["PROCEDURE_NAME2"]=zulfirenamed["PROCEDURE_NAME2"].astype(str).str.upper()
zulfirenamed["PROCEDURE_NAME2"]=zulfirenamed["PROCEDURE_NAME2"].apply(lambda x: x.replace(' ','') )


zulfirenamed["PROCEDURE_NAME_Nicp"]=zulfirenamed["PROCEDURE_NAME2"].map(procduremapping_pointsdwadme[procduremapping_pointsdwadme['NPHIES Examination Name']!="nan"].set_index('NPHIES Examination Name')['NICIP Examination Name'])

zulfirenamed.loc[zulfirenamed['PROCEDURE_NAME2']=='NAN','PROCEDURE_NAME_Nicp']=" "




zulfirenamed['Hospital']='Al Zulfi'


zulfirenamed2=pd.concat([zulfirenamed, AlZulfi_new], ignore_index=True,axis=0)  


zulfirenamed2=zulfirenamed2.drop_duplicates()
zulfirenamed2=zulfirenamed2.loc[~zulfirenamed2['PAT_NAME'].str.contains("Test","Ghazi Alreshaid")]



ph_kf_yam_ar_dw_zu= pd.concat([ph_kf_yam_ar_dw, zulfirenamed2], ignore_index=True,axis=0)  


zulfi_notmappedradio=zulfirenamed.loc[(zulfirenamed['SIGNER_Name2'].isnull())&(zulfirenamed['PROCEDURE_STATUS']=='Final')]['SIGNER_Name'].value_counts().reset_index()






###############################    Majmmah


majmmah = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Majmmah\MAJMAAH 1 SEP - 16 NOV.xlsx")



majmmah=majmmah.drop_duplicates(['Accession number'],keep="last")


majmmah['Social security number']=majmmah['Social security number'].astype(str).str.split(pat=".").str[0]

majmmah.loc[majmmah['Social security number'].str[0]=='1','Nationality']="Saudi"

majmmah.loc[majmmah['Social security number'].str[0]!='1','Nationality']="Non Saudi"


majmmah.loc[majmmah['Modality type']=='MG','Modality type']="Mamo"
majmmah.loc[majmmah['Modality type']=='MR','Modality type']="MRI"
majmmah.loc[majmmah['Modality type'].str.contains('BMD'),'Modality type']="X-Ray (BMD)"

majmmah.loc[majmmah['Modality type'].str.contains('DX'),'Modality type']="X-Ray"
majmmah.loc[majmmah['Modality type'].str.contains('CR'),'Modality type']="X-Ray"

majmmah.loc[majmmah['Modality type'].str.contains('RF'),'Modality type']="X-Ray (Fluoro)"
majmmah['Procedure name2']=majmmah['Procedure name']


majmmah.loc[majmmah['Report available']!='Report available','Report status']="Exam Ended"
majmmah.loc[majmmah['Report status']=='Validated','Report status']="Final"
majmmah.loc[majmmah['Report status']=='Preliminary','Report status']="Prelim"


majmmah['Accession number']= 'Almajmah' + '_' + majmmah['Accession number'].astype(str)
majmmah['Patient ID']= 'Almajmah' + '_' + majmmah['Patient ID'].astype(str)
majmmah['Scheduled procedure date']= majmmah['Scheduled procedure date'].astype(str) + ' ' + majmmah['Scheduled procedure time'].astype(str)

majmmah['Age']=(datetime.today() - pd.to_datetime(majmmah['Patient date of birth'])).dt.days/365
majmmahrenamed = majmmah.rename(columns=mapping.set_index('Almajmah')['PMAH'].to_dict())

majmmahrenamed = majmmahrenamed[majmmahrenamed.columns[majmmahrenamed.columns.isin (mapping['PMAH'])]]




majmmahrenamed["SIGNER_Name"]=majmmahrenamed["SIGNER_Name"].str.upper().str.strip();
majmmahrenamed.loc[majmmahrenamed['Assigned Radiologist'].isnull(),'Assigned Radiologist']=majmmahrenamed['Assigned Physicians']


majmmahrenamed["Assigned Radiologist"]=majmmahrenamed["Assigned Radiologist"].str.upper().str.strip()


Radiolgistnames["Majmaah"]=Radiolgistnames["Majmaah"].str.upper().str.strip()
Radiolgistnames.fillna(0,inplace=True)



majmmahrenamed["SIGNER_Name2"]=majmmahrenamed["SIGNER_Name"].map(Radiolgistnames[Radiolgistnames["Majmaah"]!=0].set_index("Majmaah")['Final unified list'])
majmmahrenamed["Assigned Radiologist"]=majmmahrenamed["Assigned Radiologist"].map(Radiolgistnames[Radiolgistnames["Majmaah"]!=0].set_index("Majmaah")['Final unified list'])

majmmahrenamed['PROCEDURE_END'] = majmmahrenamed['PROCEDURE_END'].apply(datetime.strptime, args=('%d-%b-%Y, %I:%M %p',))
#majmmahrenamed['PROCEDURE_END']=pd.to_datetime(majmmahrenamed['PROCEDURE_END'])
majmmahrenamed['REPORT_VERIFICATION_DATE']=pd.to_datetime(majmmahrenamed['REPORT_VERIFICATION_DATE'],format='%d-%b-%Y, %I:%M %p')



majmmahrenamed["PROCEDURE_NAME2"]=majmmahrenamed["PROCEDURE_NAME"]

majmmahrenamed["PROCEDURE_NAME2"]=majmmahrenamed["PROCEDURE_NAME2"].astype(str).str.upper()
majmmahrenamed["PROCEDURE_NAME2"]=majmmahrenamed["PROCEDURE_NAME2"].apply(lambda x: x.replace(' ','') )


majmmahrenamed["PROCEDURE_NAME_Nicp"]=majmmahrenamed["PROCEDURE_NAME2"].map(procduremapping_pointsdwadme[procduremapping_pointsdwadme['NPHIES Examination Name']!="nan"].set_index('NPHIES Examination Name')['NICIP Examination Name'])

majmmahrenamed.loc[majmmahrenamed['PROCEDURE_NAME2']=='NAN','PROCEDURE_NAME_Nicp']=" "

majmah_notmappedradio=majmmahrenamed.loc[(majmmahrenamed['SIGNER_Name2'].isnull())&(majmmahrenamed['PROCEDURE_STATUS']=='Final')]['SIGNER_Name'].value_counts().reset_index()




majmmahrenamed['Hospital']='Al Majmaah'


majmmahrenamed2=pd.concat([majmmahrenamed, Almajmah_new], ignore_index=True,axis=0)  


majmmahrenamed2=majmmahrenamed2.drop_duplicates()



ph_kf_yam_ar_dw_zu_mj2= pd.concat([ph_kf_yam_ar_dw_zu, majmmahrenamed2], ignore_index=True,axis=0)  

# ph_kf_yam_ar_dw_zu_mj['PROCEDURE_END']=pd.to_datetime(ph_kf_yam_ar_dw_zu_mj['PROCEDURE_END'],errors="coerce")
ph_kf_yam_ar_dw_zu_mj2=ph_kf_yam_ar_dw_zu_mj2.drop(['Days to Sched','DICTATION_DATE','Quality Element','Quality Priority','Quality User','Protocol Info','Quality Comment','Residents','REQUEST_NO','REQUEST_DOCTOR_LASTNAME','REQUEST_DOCTOR_FIRSTNAME','COREAD_DATE','Tech QA','Repeats','Sched Prod','Order Time','Order Date','Anesthesia','Phys Prod','ChkIn DTTM','Check-in to Appt Time (min)','Check-in to Begin (min)','Addendum Note','Peer Review Comments','Peer Review Decision','Peer Review Instant','Peer Review Reviewer','Notes','Notes.1','Study Note',], axis=1)
ph_kf_yam_ar_dw_zu_mj=ph_kf_yam_ar_dw_zu_mj2.iloc[:,:-30]
ph_kf_yam_ar_dw_zu_mj.info(50)

ph_kf_yam_ar_dw_zu_mj=pd.concat([Old_data,ph_kf_yam_ar_dw_zu_mj],ignore_index=True,axis=0)

signername_blank=ph_kf_yam_ar_dw_zu_mj.loc [(ph_kf_yam_ar_dw_zu_mj['SIGNER_Name'].isnull())&(ph_kf_yam_ar_dw_zu_mj['PROCEDURE_STATUS']=='Final')]
signername2_blamk=ph_kf_yam_ar_dw_zu_mj.loc [(ph_kf_yam_ar_dw_zu_mj['SIGNER_Name2'].isnull())&(~ph_kf_yam_ar_dw_zu_mj['SIGNER_Name'].isnull())]
asdsignedradiologist_blank=ph_kf_yam_ar_dw_zu_mj.loc [(ph_kf_yam_ar_dw_zu_mj['Assigned Radiologist'].isnull())&(ph_kf_yam_ar_dw_zu_mj['PROCEDURE_STATUS']!='Final')]



all_notmappedprocedires=ph_kf_yam_ar_dw_zu_mj.loc[ph_kf_yam_ar_dw_zu_mj['PROCEDURE_NAME_Nicp'].isnull()].groupby(['Hospital',"PROCEDURE_NAME"])['PROCEDURE_KEY'].count()
all_notmappedprocedires=all_notmappedprocedires.reset_index()


ph_kf_yam_ar_dw_zu_mj=ph_kf_yam_ar_dw_zu_mj.loc[ph_kf_yam_ar_dw_zu_mj['PERFORMING_DOCTOR']!='00400']
ph_kf_yam_ar_dw_zu_mj=ph_kf_yam_ar_dw_zu_mj.loc[ph_kf_yam_ar_dw_zu_mj['PERFORMING_DOCTOR']!='00200']

ph_kf_yam_ar_dw_zu_mj['SIGNER_Name2']=ph_kf_yam_ar_dw_zu_mj['SIGNER_Name2'].str.strip()

ph_kf_yam_ar_dw_zu_mj=ph_kf_yam_ar_dw_zu_mj.loc[~((ph_kf_yam_ar_dw_zu_mj['WORKPLACE_CODE'].isin(['MH OUTSIDE LOADS','RADWORKS PEDIA']))&(ph_kf_yam_ar_dw_zu_mj['SECTION_CODE'].isin(['X-Ray','US','X-Ray (Fluoro)'])))]

#invest=ph_kf_yam_ar_dw_zu_mj.loc[((ph_kf_yam_ar_dw_zu_mj["SIGNER_Name2"]=='Dr. Feras Essa Alomar'))& (ph_kf_yam_ar_dw_zu_mj["PROCEDURE_STATUS"]=='Final')]
all_notmappedradio=ph_kf_yam_ar_dw_zu_mj.loc[(ph_kf_yam_ar_dw_zu_mj['SIGNER_Name2'].isnull())&(ph_kf_yam_ar_dw_zu_mj['PROCEDURE_STATUS']=='Final')]['SIGNER_Name'].value_counts().reset_index()


ph_kf_yam_ar_dw_zu_mj['PROCEDURE_KEY']=ph_kf_yam_ar_dw_zu_mj['PROCEDURE_KEY'].astype(str)

ph_kf_yam_ar_dw_zu_mj['PAT_PID_NUMBER']=ph_kf_yam_ar_dw_zu_mj['PAT_PID_NUMBER'].astype(str)
ph_kf_yam_ar_dw_zu_mj['PD_MISC_NUMBER_1']=ph_kf_yam_ar_dw_zu_mj['PD_MISC_NUMBER_1'].astype(str)
ph_kf_yam_ar_dw_zu_mj['REQUEST_DATE']=ph_kf_yam_ar_dw_zu_mj['REQUEST_DATE'].astype(str)
ph_kf_yam_ar_dw_zu_mj['APPOINTMENT_DATE']=ph_kf_yam_ar_dw_zu_mj['APPOINTMENT_DATE'].astype(str)
ph_kf_yam_ar_dw_zu_mj['REPORT_VERIFICATION_DATE']=ph_kf_yam_ar_dw_zu_mj['REPORT_VERIFICATION_DATE'].astype(str)
ph_kf_yam_ar_dw_zu_mj['PP_MISC_TEXT_22']=ph_kf_yam_ar_dw_zu_mj['PP_MISC_TEXT_22'].astype(str)
ph_kf_yam_ar_dw_zu_mj['PROCEDURE_NAME_Nicp']=ph_kf_yam_ar_dw_zu_mj['PROCEDURE_NAME_Nicp'].astype(str)
ph_kf_yam_ar_dw_zu_mj['PROCEDURE_REMARK']=ph_kf_yam_ar_dw_zu_mj['PROCEDURE_REMARK'].astype(str)
ph_kf_yam_ar_dw_zu_mj['REGISTRATION_ARRIVAL']=ph_kf_yam_ar_dw_zu_mj['REGISTRATION_ARRIVAL'].astype(str)

aa=ph_kf_yam_ar_dw_zu_mj2['REGISTRATION_ARRIVAL'].value_counts()

xx=ph_kf_yam_ar_dw_zu_mj2.loc[ph_kf_yam_ar_dw_zu_mj2['REGISTRATION_ARRIVAL']=='02:05:02']
ph_kf_yam_ar_dw_zu_mj=ph_kf_yam_ar_dw_zu_mj.drop_duplicates(['PROCEDURE_KEY'])



ph_kf_yam_ar_dw_zu_mj.info()
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.Table.from_pandas(ph_kf_yam_ar_dw_zu_mj)

pq.write_table(table, r'D:\AAML\CCC\Hospitals data\sample.parquet')







# ph_kf_yam_ar_dw_zu_mj2=ph_kf_yam_ar_dw_zu_mj.columns

from openpyxl import Workbook
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# Create a Workbook object
workbook = Workbook()

# Access the active sheet
All = workbook.active

# Create the data you want to write to Excel


rows = list(dataframe_to_rows(ph_kf_yam_ar_dw_zu_mj, index=False, header=True))

# Write the data to the sheet
for row in rows:
    All.append(row)

# Specify the filename for the Excel file
filename = r'D:\AAML\CCC\Hospitals data\ph_kf_yam_ar_dw_zu_mj_'+datetime.today().strftime("%d %b, %Y")+'.xlsx'

# Save the workbook to Excel
workbook.save(filename)









######################## Start Parquet File########################





# table1 = pq.read_table(r'D:\AAML\CCC\Hospitals data\sample.parquet')

# # Convert the Arrow Table to a Pandas DataFrame
# df = table.to_pandas()

# print(df)




# kfmc_MRI=ph_kf_yam_ar_dw_zu_mj.loc[(ph_kf_yam_ar_dw_zu_mj['Hospital']=='KFMC')&(ph_kf_yam_ar_dw_zu_mj['SECTION_CODE']=='MRI')]
# kfmc_MRI.to_excel(r'D:\AAML\CCC\Hospitals data\kfmc_MRI'+datetime.today().strftime("%d %b, %Y")+'.xlsx', sheet_name = "All", index = False) 

# ph_kf_yam_ar_dw_zu_mj.to_excel(r'D:\AAML\CCC\Hospitals data\ph_kf_yam_ar_dw_zu_mj'+datetime.today().strftime("%d %b, %Y")+'.xlsx', sheet_name = "All", index = False) 

# print(datetime.now())
# ph_kf_yam_ar_dw_zu_mj.info()

# ##################################### 

# ph_kf_yam_ar_dw_zu_mj.to_csv(r'D:\AAML\CCC\Hospitals data\ph_kf_yam_ar_dw_zu_mj'+datetime.today().strftime("%d %b, %Y")+'.csv', index = False) 

# ##############################

