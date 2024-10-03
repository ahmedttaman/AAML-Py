# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 12:13:55 2024

@author: Ahmad
"""



import pandas as pd
import numpy as np
from datetime import datetime, date
import os
import datetime as dt

#Raa = pd.read_csv (r"C:\Users\Ahmad\Desktop\240 # New Monday.csv")


#############################master Tables
Radiolgistnames = pd.read_excel(r"D:\AAML\CCC\Hospitals data\ALL RADIOLOGOSITS MAPPED NAMES August.xlsx")
Radiolgistnames.fillna(0,inplace=True)

mapping = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Mapping\hospital mapping.xlsx")

procduremapping_points = pd.read_excel(r"D:\AAML\CCC\Hospitals data\Radiologist Productivity\NPHIES Points System modified 21-4-2024.xlsx",sheet_name="KFMC to NICIP to NPHIES Mapping")

pmah_stafff= pd.read_excel(r"D:\AAML\CCC\Hospitals data\PMAH\All Staff V 29 April.xlsx")
#procduremapping_points.info()



alymamh = pd.read_excel(r"D:\AAML\CCC\Hospitals data\AlYamamh\Yammamah 01-may-01-oct.xlsx")

alym_proc_mapping = pd.read_excel(r"D:\AAML\CCC\Hospitals data\NAPHIS Imaging Procedures 25 AUG 23.xlsx",sheet_name="Imaging Procedure")

alymamh=pd.merge(alymamh, alym_proc_mapping,left_on="Tamar Procedure Code",right_on="Code", how="left")
alymamh.loc[alymamh['Description'].isnull(),'Description']=alymamh['Procedure Name']



# alymamh.to_excel(r'D:\AAML\CCC\Hospitals data\alymamh'+datetime.today().strftime("%d %b, %Y")+'.xlsx', sheet_name = "All", index = False) 

# # because I addedd the type means admission type manauly in the file 
# alymamh = pd.read_excel(r"D:\AAML\CCC\Hospitals data\alymamh01 Feb, 2024.xlsx")
alymamh['Scan Datetime']=pd.to_datetime( alymamh['Date'].astype(str) + ' ' + alymamh['Time'].astype(str))

alymamh=alymamh.loc[alymamh['Scan Datetime']<datetime.strptime('06/06/24 00:00:01', '%m/%d/%y %H:%M:%S')]


alymamh=alymamh.drop_duplicates(['Accession'],keep="last")





alymamh.info()

alymamh=alymamh[alymamh['Status']!='Cancel']

# mapping admission type 
alymamh.loc[alymamh['Custom Field 1']=='Emergency','Type']="E"
alymamh.loc[alymamh['Custom Field 1']=='OutPatient','Type']="O"
alymamh.loc[alymamh['Custom Field 1']=='InPatient','Type']="I"


# mapping modalitiy with contract
alymamh.loc[alymamh['Mod.'].str.contains('DX'),'contract_modality']="X-Ray"
alymamh.loc[alymamh['Mod.'].str.contains('CR'),'contract_modality']="X-Ray"
alymamh.loc[alymamh['Mod.'].str.contains('CT'),'contract_modality']="CT"
alymamh.loc[alymamh['Mod.'].str.contains('RF'),'contract_modality']="X-Ray (Fluoro)"
alymamh.loc[alymamh['Mod.'].str.contains('MR'),'contract_modality']="MRI"
alymamh.loc[alymamh['Mod.'].str.contains('US'),'contract_modality']="US"
alymamh.loc[alymamh['Description'].str.contains('Mammo',case=False),'contract_modality']="Mamo"


# Nationality
alymamh['Patient ID']=alymamh['Patient ID'].astype(str).str.split(pat=".").str[0]

alymamh.loc[alymamh['Patient ID'].str[0]=='1','Nationality']="Saudi"

alymamh.loc[alymamh['Patient ID'].str[0]!='1','Nationality']="Non Saudi"



alymamh.loc[~alymamh['Status'].str.contains('Final'),'Status']="Exam Ended"
alymamh.loc[alymamh['Status'].str.contains('Final'),'Status']="Final"



alymamh['Scan Datetime']=pd.to_datetime( alymamh['Date'].astype(str) + ' ' + alymamh['Time'].astype(str))

#alymamh.loc[alymamh['Scan Datetime']=='Final','REPORT_VERIFICATION_DATE']=alymamh['Scan Datetime']

#alymamh['Scan Datetime']=pd.to_datetime(alymamh['Scan Datetime'].str.replace('.','-'),dayfirst=True)
alymamh['Age']=(datetime.today() - alymamh['Birth Date']).dt.days/365
#alymamh['Age']=20
alymamh['Description2']=alymamh['Description']


alymamh['Accession']= 'ALYMAMH' + '_' + alymamh['Accession'].astype(str)
alymamh['ID']= 'ALYMAMH' + '_' + alymamh['Patient ID'].astype(str)
alymamhrenamed = alymamh.rename(columns=mapping.set_index('AlYamamh_old')['PMAH'].to_dict())
alymamhrenamed = alymamhrenamed[alymamhrenamed.columns[alymamhrenamed.columns.isin (mapping['PMAH'])]]

alymamhrenamed["SIGNER_Name"]=alymamhrenamed["SIGNER_Name"].str.upper().str.strip()

alymamhrenamed["Assigned Radiologist"]=alymamhrenamed["Assigned Radiologist"].str.upper().str.strip()


Radiolgistnames["Old_Yamamah"]=Radiolgistnames["Old_Yamamah"].str.upper().str.strip()
Radiolgistnames.fillna(0,inplace=True)


alymamhrenamed["SIGNER_Name2"]=alymamhrenamed["SIGNER_Name"].map(Radiolgistnames[Radiolgistnames["Old_Yamamah"]!=0].set_index("Old_Yamamah")['Final unified list'])
alymamhrenamed["Assigned Radiologist"]=alymamhrenamed["Assigned Radiologist"].map(Radiolgistnames[Radiolgistnames["Old_Yamamah"]!=0].set_index("Old_Yamamah")['Final unified list'])


procduremapping_pointsdwadme=procduremapping_points[['NPHIES Examination Name','NICIP Examination Name']]
procduremapping_pointsdwadme.fillna(0,inplace=True)
procduremapping_pointsdwadme=procduremapping_pointsdwadme.loc[(procduremapping_pointsdwadme['NPHIES Examination Name']!=0)&(procduremapping_pointsdwadme['NICIP Examination Name']!=0)&(procduremapping_pointsdwadme['NPHIES Examination Name']!=" ")&(procduremapping_pointsdwadme['NPHIES Examination Name']!="  ")&(procduremapping_pointsdwadme['NPHIES Examination Name'].str.strip()!=" ")]
procduremapping_pointsdwadme['NPHIES Examination Name']=procduremapping_pointsdwadme['NPHIES Examination Name'].astype(str)
procduremapping_pointsdwadme['NPHIES Examination Name']=procduremapping_pointsdwadme['NPHIES Examination Name'].str.upper()
procduremapping_pointsdwadme['NPHIES Examination Name']=procduremapping_pointsdwadme['NPHIES Examination Name'].apply(lambda x: x.replace(' ','') )
procduremapping_pointsdwadme=procduremapping_pointsdwadme.drop_duplicates(['NPHIES Examination Name'])


alymamhrenamed["PROCEDURE_NAME2"]=alymamhrenamed["PROCEDURE_NAME"]

alymamhrenamed["PROCEDURE_NAME2"]=alymamhrenamed["PROCEDURE_NAME2"].astype(str).str.upper()
alymamhrenamed["PROCEDURE_NAME2"]=alymamhrenamed["PROCEDURE_NAME2"].apply(lambda x: x.replace(' ','') )

alymamhrenamed["PROCEDURE_NAME_Nicp"]=alymamhrenamed["PROCEDURE_NAME2"].map(procduremapping_pointsdwadme[procduremapping_pointsdwadme['NPHIES Examination Name']!="nan"].set_index('NPHIES Examination Name')['NICIP Examination Name'])

alymamhrenamed.loc[alymamhrenamed['PROCEDURE_NAME2']=='NAN','PROCEDURE_NAME_Nicp']=" "
alymamhrenamed['REPORT_VERIFICATION_DATE']=pd.to_datetime(alymamhrenamed['REPORT_VERIFICATION_DATE'],format='%m/%d/%Y %I:%M:%S %p')



alymamhrenamed['Hospital']='Al Yamamah'

alymamhrenamed.to_excel(r'D:\AAML\CCC\Hospitals data\AlYamamh\Alyamamh_OldRis_'+datetime.today().strftime("%d %b, %Y")+'.xlsx', sheet_name = "All", index = False) 



# #phah_kfmc_yamamh.to_excel(r'D:\AAML\CCC\Hospitals data\phah_kfmc_ymamh'+datetime.today().strftime("%d %b, %Y")+'.xlsx', sheet_name = "All", index = False) 
# Alymamh_notmappedprocedires=alymamhrenamed.loc[alymamhrenamed['PROCEDURE_NAME_Nicp'].isnull()]['PROCEDURE_NAME'].value_counts().reset_index()

# Alymamh_notmappedradio=alymamhrenamed.loc[(alymamhrenamed['SIGNER_Name2'].isnull())&(alymamhrenamed['PROCEDURE_STATUS']=='Final')]['SIGNER_Name'].value_counts().reset_index()
# noof=alymamhrenamed.loc[(alymamhrenamed['SIGNER_Name']=='DR NOOF ALENEZI')]

