# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 15:06:17 2024

@author: Ahmad
"""




import pandas as pd

import numpy as np

from datetime import datetime

import os

import datetime as dt

 

 


# path_lookup = path_wasfaty+"Lookups/"

# path_data = path_wasfaty+"937/sharepoint/"

path_raw = "C://Users\Ahmad\Box\AAML Daily Files//"

 

files = os.listdir(path_raw)

 

"""

Read sheets:

*

*

"""

df=pd.DataFrame()

i=1

for file in os.listdir(path_raw):

    if file.startswith("Alliance_Saudi") and file.endswith(".csv") and i <2:
        temp=pd.DataFrame()

        temp = pd.read_csv(path_raw+"Alliance_Saudi_20240430_1400.csv",on_bad_lines='skip')

        print(file)
        df=pd.concat([df, temp])
        i=i+1




ickets = pd.read_csv(r"C:\Users\Ahmad\Box\AAML Daily Files\Alliance_Saudi_20240710_1000.csv.")
