# -*- coding: utf-8 -*-
"""
Created on Sun Oct 20 10:55:48 2024

@author: Ahmad
"""

import pandas as pd
from ydata_profiling import ProfileReport

df = pd.read_excel('D:\AAML\CCC\Hospitals data\AlYamamh\C2 2 Nov Exam.xlsx')
profile = ProfileReport(df, title="Profiling Report")

profile.to_file("D:\AAML\CCC\Hospitals data\Satisfaction\C2.html")