# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 08:15:20 2022

@author: ifraga
"""

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'


import pandas
import matplotlib.pyplot as plt
import pandas.io.sql as psql
from sqlalchemy import create_engine

import FUNCIONES_PROCESADO
import numpy


import seawater


archivo_AA   = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/211129_RCAN19_ENE_costR1R1.xlsx'
#archivo_AA   = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/221003_RCAN19_ENE_costR1R1.xlsx'


archivo_datos_procesados = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/RCAN2019_REVISADO.xlsx'


rendimiento_columna     = 100
temperatura_laboratorio = 16.8
rmn_elegida             = 'RADCAN COSTERAS 2019'

# Variables
variables_run = ['ton','nitrito','silicato','fosfato'] 

con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
df_muestreos            = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
df_datos_fisicos        = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn_psql)
df_rmns                 = psql.read_sql('SELECT * FROM rmn_nutrientes', conn_psql)
df_datos_biogeoquimicos = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn_psql)
conn_psql.dispose()


df_referencias           = df_rmns[df_rmns['nombre_rmn']==rmn_elegida]







# Lectura del archivo con los resultados del AA
datos_AA              = pandas.read_excel(archivo_AA,skiprows=15)            
datos_AA              = datos_AA.rename(columns={"Results 1":variables_run[0],"Results 2":variables_run[1],"Results 3":variables_run[2],"Results 4":variables_run[3]})
      
# Añade la información de salinidad en aquellas muestras que tienen un muestreo asociado
df_muestreos          = df_muestreos.rename(columns={"id_muestreo":"muestreo"}) # Para igualar los nombres de columnas                                               
df_datos_disponibles  = pandas.merge(df_datos_fisicos, df_muestreos, on="muestreo")            

# Encuentra las posiciones de las referencias de sw
indices_referencias = numpy.asarray(datos_AA['Peak Number'][datos_AA['Sample ID']=='sw']) - 1
# Agrupa en dos tandas, las iniciales y las finales
spl          = [0]+[i for i in range(1,len(indices_referencias)) if indices_referencias[i]-indices_referencias[i-1]>1]+[None]
listado_refs = [indices_referencias[b:e] for (b, e) in [(spl[i-1],spl[i]) for i in range(1,len(spl))]]

ref_inicial        = listado_refs[0][-1] + 1
ref_final          = listado_refs[1][0]

# Encuentra la salinidad de cada muestra
datos_AA['salinidad']     = numpy.ones(datos_AA.shape[0])
datos_AA['io_procesado']  = None
for idato in range(ref_inicial,ref_final):
    if datos_AA['Cup Type'].iloc[idato] == 'SAMP':
 
        id_temp = df_datos_disponibles['muestreo'][df_datos_disponibles['nombre_muestreo']==datos_AA['Sample ID'].iloc[idato]]
            
        if len(id_temp) > 0:
            datos_AA['salinidad'].iloc[idato]     = df_datos_disponibles['salinidad_ctd'][df_datos_disponibles['muestreo']==id_temp.iloc[0]]
            datos_AA['io_procesado'].iloc[idato]  = 1
        else:
            texto_error = 'La muestra ' + datos_AA['Sample ID'].iloc[idato] + ' no está inlcluida en la base de datos y no ha sido procesada'
            #st.warning(texto_error, icon="⚠️")                        
   
# comprobación por si no hay ningún dato a procesar
if datos_AA['io_procesado'].isnull().all():

    ########
    print('No hay datos')
    ########
    
         
   
else:
    
# En caso contrario procesa los datos

    # Aplica la corrección de deriva (DRIFT)                 
    datos_corregidos = FUNCIONES_PROCESADO.correccion_drift(datos_AA,df_referencias,variables_run,rendimiento_columna,temperatura_laboratorio)
    
    # Calcula el NO3 como diferencia entre el TON y el NO2
    datos_corregidos['nitrato'] = datos_corregidos['ton'] - datos_corregidos['nitrito']

    # corrige posibles valores negativos
    datos_corregidos['nitrato'][datos_corregidos['nitrato']<0]   = 0
    datos_corregidos['nitrito'][datos_corregidos['nitrito']<0]   = 0
    datos_corregidos['silicato'][datos_corregidos['silicato']<0] = 0
    datos_corregidos['fosfato'][datos_corregidos['fosfato']<0]   = 0

    ########
    # texto_exito = 'Muestreos disponibles procesados correctamente'
    # st.success(texto_exito)
    ########


                   



    # COMPARA CON PROCESADO "TRADICIONAL"
    
    # Lectura de los datos obtenidos con la hoja excel
    df_datos_disponibles         = pandas.read_excel(archivo_datos_procesados,skiprows=1)            
    df_datos_disponibles         = df_datos_disponibles.rename(columns={'salbtl':'salinidad_ctd','ID':'nombre_muestreo'})

    indices_dataframe        = numpy.arange(0,df_datos_disponibles.shape[0],1,dtype=int)    
    df_datos_disponibles['muestreo'] = indices_dataframe
    df_datos_disponibles.set_index('muestreo',drop=False,append=False,inplace=True)
    
    
    df_datos_disponibles  = df_datos_disponibles.rename(columns={'NO3+NO2.1':'ton_proc','NO2.1':'nitrito_proc','SiO2.1':'silicato_proc','PO4.1':'fosfato_proc'})
    df_datos_disponibles_comparacion  = df_datos_disponibles[['ton_proc','nitrito_proc','silicato_proc','fosfato_proc','nombre_muestreo']]


    tabla_comparacion = pandas.merge(df_datos_disponibles_comparacion, datos_corregidos, on="nombre_muestreo")   

    # Create two subplots and unpack the output array immediately
    # f, (ax1, ax2,ax3,ax4) = plt.subplots(2, 2, sharey=True)
    # ax1.scatter(tabla_comparacion['ton'], 'ton_proc')
    # ax1.plot([min(tabla_comparacion['ton']),max(tabla_comparacion['ton'])], [min(tabla_comparacion['ton']),max(tabla_comparacion['ton'])])
    f, axs = plt.subplots(2, 2)
    axs[0, 0].scatter(tabla_comparacion['ton'], tabla_comparacion['ton_proc'],marker='.')
    axs[0, 0].plot([min(tabla_comparacion['ton']),max(tabla_comparacion['ton'])], [min(tabla_comparacion['ton']),max(tabla_comparacion['ton'])])
    axs[0, 0].set_title('TON')

    axs[0, 1].scatter(tabla_comparacion['nitrito'], tabla_comparacion['nitrito_proc'],marker='.')
    axs[0, 1].plot([min(tabla_comparacion['nitrito']),max(tabla_comparacion['nitrito'])], [min(tabla_comparacion['nitrito']),max(tabla_comparacion['nitrito'])])
    axs[0, 1].set_title('NITRITO')
    
    axs[1, 0].scatter(tabla_comparacion['silicato'], tabla_comparacion['silicato_proc'],marker='.')
    axs[1, 0].plot([min(tabla_comparacion['silicato']),max(tabla_comparacion['silicato'])], [min(tabla_comparacion['silicato']),max(tabla_comparacion['silicato'])])
    axs[1, 0].set_title('SILICATO')
    
    axs[1, 1].scatter(tabla_comparacion['fosfato'], tabla_comparacion['fosfato_proc'],marker='.')
    axs[1, 1].plot([min(tabla_comparacion['fosfato']),max(tabla_comparacion['fosfato'])], [min(tabla_comparacion['fosfato']),max(tabla_comparacion['fosfato'])])
    axs[1, 1].set_title('FOSFATO')  
  
  
    
#     fig, axs = plt.subplots(2, 2, subplot_kw=dict(projection="polar"))
# axs[0, 0].plot(x, y)
# axs[1, 1].scatter(x, y)
    