# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 08:15:20 2022

@author: ifraga
"""

import numpy
import pandas
import datetime


n_dias         = 10
fecha_inicio   = datetime.date.today()
listado_fechas = [fecha_inicio + datetime.timedelta(days=x) for x in range(n_dias)]

df = pandas.DataFrame()
for idia in range(n_dias):
    df[idia]  = listado_fechas
    
df = df.transpose()
    