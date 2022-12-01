# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 08:15:20 2022

@author: ifraga
"""

import numpy
import pandas
import datetime


archivo_AA = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/PRUEBAS STREAMLIT/210212 - copia.xlsx'
archivo_refs = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/PRUEBAS STREAMLIT/210212_REFERENCIAS.xlsx'
temperatura_laboratorio = 12

# Lectura del archivo con los resultados del AA
datos_AA=pandas.read_excel(archivo_AA,skiprows=15)

df_referencias        = pandas.read_excel(archivo_refs)



datos_AA['densidad']    = numpy.ones(datos_AA.shape[0])

indices_referencias = numpy.asarray(datos_AA['Peak Number'][datos_AA['Sample ID']=='sw']) - 1

spl          = [0]+[i for i in range(1,len(indices_referencias)) if indices_referencias[i]-indices_referencias[i-1]>1]+[None]
listado_refs = [indices_referencias[b:e] for (b, e) in [(spl[i-1],spl[i]) for i in range(1,len(spl))]]

ref_inicial  = listado_refs[0][-1]
ref_final    = listado_refs[1][0]


indices_rmns_altos = numpy.asarray(datos_AA['Peak Number'][datos_AA['Sample ID']=='RMN High']) - 1
indices_rmns_bajos = numpy.asarray(datos_AA['Peak Number'][datos_AA['Sample ID']=='RMN Low']) - 1

datos_AA['densidad'].iloc[indices_rmns_bajos]  = (999.1+0.77*((df_referencias['Sal'][0])-((temperatura_laboratorio-15)/5.13)-((temperatura_laboratorio-15)**2)/128))/1000
datos_AA['densidad'].iloc[indices_rmns_altos]  = (999.1+0.77*((df_referencias['Sal'][1])-((temperatura_laboratorio-15)/5.13)-((temperatura_laboratorio-15)**2)/128))/1000

for idato in range(ref_inicial,ref_final):
    
    


# data = [1, 4, 5, 6, 10, 15, 16, 17, 18, 22, 25, 26, 27, 28]
# >>> spl = [0]+[i for i in range(1,len(data)) if data[i]-data[i-1]>1]+[None]
# >>> [data[b:e] for (b, e) in [(spl[i-1],spl[i]) for i in range(1,len(spl))]]