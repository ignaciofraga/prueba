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


archivo_AA   = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/PRUEBAS STREAMLIT/RADCAN2017/220809A_RADACAN2017profR1R1.xlsx'

#archivo_resultados   = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/PRUEBAS STREAMLIT/RADCOR_2022.xlsm'
#datos_res            = pandas.read_excel(archivo_resultados,sheet_name='DatosFinales',skiprows=1) 



rendimiento_columna     = 100
temperatura_laboratorio = 19.9


con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
df_rmns_bajos    = psql.read_sql('SELECT * FROM rmn_bajo_nutrientes', conn_psql)
df_rmns_altos    = psql.read_sql('SELECT * FROM rmn_alto_nutrientes', conn_psql)
df_muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
df_estaciones             = psql.read_sql('SELECT * FROM estaciones', conn_psql)
df_datos_biogeoquimicos   = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn_psql)
df_datos_fisicos          = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn_psql)

conn_psql.dispose()   

df_referencias_altas = df_rmns_altos[df_rmns_altos['nombre_rmn']=='CH']
df_referencias_bajas = df_rmns_bajos[df_rmns_bajos['nombre_rmn']=='CE']

# Define los vectores con las variables a procesar
variables_procesado    = ['TON','Nitrato','Nitrito','Silicato','Fosfato']    
variables_procesado_bd = ['ton','nitrato','nitrito','silicato','fosfato']
variables_unidades     = ['\u03BCmol/kg','\u03BCmol/kg','\u03BCmol/kg','\u03BCmol/kg','\u03BCmol/kg']
variables_run = ['ton','nitrito','silicato','fosfato']


# Lectura del archivo con los resultados del AA
datos_AA              = pandas.read_excel(archivo_AA,skiprows=15)            
datos_AA              = datos_AA.rename(columns={"Results 1":variables_run[0],"Results 2":variables_run[1],"Results 3":variables_run[2],"Results 4":variables_run[3]})
      
# Añade la información de salinidad en aquellas muestras que tienen un muestreo asociado                                            
df_datos_disponibles  = pandas.merge(df_datos_fisicos, df_muestreos, on="muestreo")            

# Adapta el nombre de las sw
for idato in range(datos_AA.shape[0]):
    if datos_AA['Sample ID'].iloc[idato][0:2].lower()=='sw':
       datos_AA['Sample ID'].iloc[idato] ='sw' 
        
# Encuentra las posiciones de las referencias de sw
indices_referencias = numpy.asarray(datos_AA['Peak Number'][datos_AA['Sample ID']=='sw']) - 1
# Agrupa en dos tandas, las iniciales y las finales
spl          = [0]+[i for i in range(1,len(indices_referencias)) if indices_referencias[i]-indices_referencias[i-1]>1]+[None]
listado_refs = [indices_referencias[b:e] for (b, e) in [(spl[i-1],spl[i]) for i in range(1,len(spl))]]

ref_inicial        = listado_refs[0][-1] + 1
ref_final          = listado_refs[1][0]

# # Encuentra la salinidad de cada muestra
# datos_AA['salinidad']     = numpy.ones(datos_AA.shape[0])
# datos_AA['io_procesado']  = None
# for idato in range(ref_inicial,ref_final):
#     if datos_AA['Cup Type'].iloc[idato] == 'SAMP':
 
#         id_temp = df_datos_disponibles['muestreo'][df_datos_disponibles['id_externo']==datos_AA['Sample ID'].iloc[idato]]
            
#         if len(id_temp) > 0:
#             datos_AA['salinidad'].iloc[idato]     = df_datos_disponibles['salinidad_ctd'][df_datos_disponibles['muestreo']==id_temp.iloc[0]]
#             datos_AA['io_procesado'].iloc[idato]  = 1
#         # else:
#         #     texto_error = 'La muestra ' + datos_AA['Sample ID'].iloc[idato] + ' no está inlcluida en la base de datos y no ha sido procesada'
#         #     st.warning(texto_error, icon="⚠️")                        
   
# # comprobación por si no hay ningún dato a procesar
# if datos_AA['io_procesado'].isnull().all():
#     texto_error = "Ninguna de las muestras analizadas se corresponde con muestreos incluidos en la base de datos"
#     # st.warning(texto_error, icon="⚠️")          
   
# else:
    
# # En caso contrario procesa los datos
            
#     # # Aplica la corrección de deriva (DRIFT)                 
#     # datos_corregidos = FUNCIONES_PROCESADO.correccion_drift(datos_AA,df_referencias_altas,df_referencias_bajas,variables_run,rendimiento_columna,temperatura_laboratorio)
#     datos_entrada = datos_AA                
    
    
#     # Predimensiona un dataframe con los resultados de la correccion
#     datos_corregidos = pandas.DataFrame(columns=variables_run)    

#     # Encuentra los índices (picos) correspondientes a la calbración
#     indices_calibracion = numpy.asarray(datos_entrada['Peak Number'][datos_entrada['Cup Type']=='CALB']) - 1
           
#     # Corrige las concentraciones a partir de los rendimientos de la coumna reductora
#     datos_entrada['nitrato_rendimiento'] = numpy.zeros(datos_entrada.shape[0])
#     datos_entrada['ton_rendimiento'] = numpy.zeros(datos_entrada.shape[0])
#     factor = ((datos_entrada['ton'].iloc[indices_calibracion[-1]]*rendimiento_columna/100) + datos_entrada['nitrito'].iloc[indices_calibracion[-1]])/(datos_entrada['ton'].iloc[indices_calibracion[-1]] + datos_entrada['nitrito'].iloc[indices_calibracion[-1]])
#     for idato in range(datos_entrada.shape[0]):
#         datos_entrada['nitrato_rendimiento'].iloc[idato] = (datos_entrada['ton'].iloc[idato]*factor - datos_entrada['nitrito'].iloc[idato])/(rendimiento_columna/100) 
#         datos_entrada['ton_rendimiento'].iloc[idato] = datos_entrada['nitrato_rendimiento'].iloc[idato] + datos_entrada['nitrito'].iloc[idato]
    
#     # Asocia la temperatura de laboratorio a todas las muestras
#     datos_entrada['temp.lab'] = temperatura_laboratorio
    
#     # Pasa las concentraciones a mol/kg
#     posicion_RMN_bajos = numpy.zeros(2,dtype=int)
#     posicion_RMN_altos = numpy.zeros(2,dtype=int)
#     icont_bajos        = 0
#     icont_altos        = 0


#     for idato in range(datos_entrada.shape[0]):
#         if datos_entrada['Sample ID'].iloc[idato][0:7].lower() == 'rmn low' :
#             posicion_RMN_bajos[icont_bajos] = idato
#             icont_bajos                     = icont_bajos + 1 
#             datos_entrada['salinidad'].iloc[idato]  = df_referencias_bajas['salinidad'].iloc[0]
#         if datos_entrada['Sample ID'].iloc[idato][0:8].lower() == 'rmn high':
#             posicion_RMN_altos[icont_altos] = idato
#             icont_altos                     = icont_altos + 1
#             datos_entrada['salinidad'].iloc[idato]  = df_referencias_altas['salinidad'].iloc[0]

#     densidades = seawater.eos80.dens0(datos_entrada['salinidad'], datos_entrada['temp.lab'])
#     datos_entrada['DENSIDAD'] = densidades/1000  
                    
#     datos_entrada['ton_CONC'] = datos_entrada['ton_rendimiento']/datos_entrada['DENSIDAD']  
#     datos_entrada['nitrato_CONC'] = datos_entrada['nitrato_rendimiento']/datos_entrada['DENSIDAD']  
#     datos_entrada['nitrito_CONC'] = datos_entrada['nitrito']/datos_entrada['DENSIDAD']  
#     datos_entrada['silicato_CONC'] = datos_entrada['silicato']/datos_entrada['DENSIDAD']  
#     datos_entrada['fosfato_CONC'] = datos_entrada['fosfato']/datos_entrada['DENSIDAD']  


    
#     ####  APLICA LA CORRECCIÓN DE DERIVA ####
#     # Encuentra las posiciones de los RMNs
#     #posicion_RMN_bajos  = [i for i, e in enumerate(datos_entrada['Sample ID']) if e == 'RMN Low']
#     #posicion_RMN_altos  = [i for i, e in enumerate(datos_entrada['Sample ID']) if e == 'RMN High']
    
#     for ivariable in range(len(variables_run)):
               
#         variable_concentracion  = variables_run[ivariable] + '_CONC'
        
#         # Concentraciones de las referencias
#         #variable_rmn    = variables_run[ivariable] + '_rmn_bajo'
#         # RMN_CE_variable = df_referencias[variables_run[ivariable]].iloc[0]
#         # RMN_CI_variable = df_referencias[variables_run[ivariable]].iloc[1]  
#         RMN_BAJO_variable = df_referencias_bajas[variables_run[ivariable]].iloc[0]
#         RMN_ALTO_variable   = df_referencias_altas[variables_run[ivariable]].iloc[0]  
        
#         # Concentraciones de las muestras analizadas como referencias
#         RMN_altos       = datos_entrada[variable_concentracion][posicion_RMN_altos]
#         RMN_bajos       = datos_entrada[variable_concentracion][posicion_RMN_bajos]
            
#         # Predimensiona las rectas a y b
#         indice_min_correccion = min(posicion_RMN_altos[0],posicion_RMN_bajos[0])
#         indice_max_correccion = max(posicion_RMN_altos[1],posicion_RMN_bajos[1])
#         recta_at              = numpy.zeros(datos_entrada.shape[0])
#         recta_bt              = numpy.zeros(datos_entrada.shape[0])
            
#         pte_RMN      = (RMN_ALTO_variable-RMN_BAJO_variable)/(RMN_altos.iloc[0]-RMN_bajos.iloc[0]) 
#         t_indep_RMN  = RMN_BAJO_variable- pte_RMN*RMN_bajos.iloc[0] 
    
#         variable_drift = numpy.zeros(datos_entrada.shape[0])
    
#         # Aplica la corrección basada de dos rectas, descrita en Hassenmueller
#         for idato in range(indice_min_correccion,indice_max_correccion):

#             factor_f        = (idato-posicion_RMN_bajos[0])/(posicion_RMN_bajos[1]-posicion_RMN_bajos[0])
#             recta_at[idato] = RMN_bajos.iloc[0] +  factor_f*(RMN_bajos.iloc[-1]-RMN_bajos.iloc[0]) 
            
#             factor_f        = (idato-posicion_RMN_altos[0])/(posicion_RMN_altos[1]-posicion_RMN_altos[0])
#             recta_bt[idato] = RMN_altos.iloc[0] +  factor_f*(RMN_altos.iloc[-1]-RMN_altos.iloc[0]) 
    
#             val_combinado   = ((datos_entrada[variable_concentracion][idato]-recta_at[idato])/(recta_bt[idato]-recta_at[idato]))*(RMN_altos.iloc[0]-RMN_bajos.iloc[0]) + RMN_bajos.iloc[0]
                 
#             variable_drift[idato] = val_combinado*pte_RMN+t_indep_RMN
    
#         variable_drift[variable_drift<0] = 0
        
#         datos_corregidos[variables_run[ivariable]] = variable_drift
        
#     # Añade columna con el identificador de cada muestra
#     datos_corregidos['id_externo'] = datos_entrada['Sample ID']
    
    
    
    
    
    
    
    
    
    
#     # Corrige posibles valores negativos
#     datos_corregidos['ton'][datos_corregidos['ton']<0]   = 0
#     datos_corregidos['nitrito'][datos_corregidos['nitrito']<0]   = 0
#     datos_corregidos['silicato'][datos_corregidos['silicato']<0] = 0
#     datos_corregidos['fosfato'][datos_corregidos['fosfato']<0]   = 0

#     # Calcula el NO3 como diferencia entre el TON y el NO2
#     datos_corregidos['nitrato'] = datos_corregidos['ton'] - datos_corregidos['nitrito']
#     datos_corregidos['nitrato'][datos_corregidos['nitrato']<0]   = 0
    
#     # vuelvo a calcular el TON como NO3+NO2, por si hubiese corregido valores nulos
#     datos_corregidos['ton'] = datos_corregidos['nitrato'] + datos_corregidos['nitrito']

#     # Añade informacion de RMNs, temperaturas y rendimiento
#     datos_corregidos['rto_columna_procesado']  = rendimiento_columna
#     datos_corregidos['temp_lab_procesado']     = temperatura_laboratorio
#     datos_corregidos['rmn_bajo_procesado']     = int(df_referencias_bajas['id_rmn'].iloc[0])
#     datos_corregidos['rmn_alto_procesado']     = int(df_referencias_altas['id_rmn'].iloc[0])  
    
    
#     # Añade información de la base de datos (muestreo, biogeoquimica y fisica)
#     datos_corregidos = pandas.merge(datos_corregidos, df_muestreos, on="id_externo") # Esta unión elimina los registros que NO son muestras
    
#     df_datos_biogeoquimicos = df_datos_biogeoquimicos.drop(columns=variables_procesado_bd) # Para eliminar las columnas previas con datos de nutrientes
#     datos_corregidos = pandas.merge(datos_corregidos, df_datos_biogeoquimicos, on="muestreo",how='left')
    
#     datos_corregidos = pandas.merge(datos_corregidos, df_datos_fisicos, on="muestreo",how='left')  
    
    
    
    
    
#     datos_recorte = datos_corregidos[['id_externo','ton','nitrito','silicato','fosfato','temperatura_ctd','salinidad_ctd']]
    
#     datos_recorte['ton_proc']      = [None]*datos_recorte.shape[0]
#     datos_recorte['nitrito_proc']  = [None]*datos_recorte.shape[0]
#     datos_recorte['silicato_proc'] = [None]*datos_recorte.shape[0]
#     datos_recorte['fosfato_proc']  = [None]*datos_recorte.shape[0]
#     for idato in range(datos_recorte.shape[0]):
#         for idato_res in range(datos_res.shape[0]):
#             if datos_recorte['id_externo'][idato] == datos_res['ID'][idato_res]:
#                 datos_recorte['ton_proc'][idato] = datos_res['NO3+NO2'][idato_res]
#                 datos_recorte['nitrito_proc'][idato] = datos_res['NO2'][idato_res]
#                 datos_recorte['silicato_proc'][idato] = datos_res['SiO2'][idato_res]
#                 datos_recorte['fosfato_proc'][idato] = datos_res['PO4'][idato_res]
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

    # # Create two subplots and unpack the output array immediately
    # # f, (ax1, ax2,ax3,ax4) = plt.subplots(2, 2, sharey=True)
    # # ax1.scatter(tabla_comparacion['ton'], 'ton_proc')
    # # ax1.plot([min(tabla_comparacion['ton']),max(tabla_comparacion['ton'])], [min(tabla_comparacion['ton']),max(tabla_comparacion['ton'])])
    # f, axs = plt.subplots(2, 2)
    # axs[0, 0].scatter(tabla_comparacion['ton'], tabla_comparacion['ton_proc'],marker='.')
    # axs[0, 0].plot([min(tabla_comparacion['ton']),max(tabla_comparacion['ton'])], [min(tabla_comparacion['ton']),max(tabla_comparacion['ton'])])
    # axs[0, 0].set_title('TON')

    # axs[0, 1].scatter(tabla_comparacion['nitrito'], tabla_comparacion['nitrito_proc'],marker='.')
    # axs[0, 1].plot([min(tabla_comparacion['nitrito']),max(tabla_comparacion['nitrito'])], [min(tabla_comparacion['nitrito']),max(tabla_comparacion['nitrito'])])
    # axs[0, 1].set_title('NITRITO')
    
    # axs[1, 0].scatter(tabla_comparacion['silicato'], tabla_comparacion['silicato_proc'],marker='.')
    # axs[1, 0].plot([min(tabla_comparacion['silicato']),max(tabla_comparacion['silicato'])], [min(tabla_comparacion['silicato']),max(tabla_comparacion['silicato'])])
    # axs[1, 0].set_title('SILICATO')
    
    # axs[1, 1].scatter(tabla_comparacion['fosfato'], tabla_comparacion['fosfato_proc'],marker='.')
    # axs[1, 1].plot([min(tabla_comparacion['fosfato']),max(tabla_comparacion['fosfato'])], [min(tabla_comparacion['fosfato']),max(tabla_comparacion['fosfato'])])
    # axs[1, 1].set_title('FOSFATO')  
  
  
    
#     fig, axs = plt.subplots(2, 2, subplot_kw=dict(projection="polar"))
# axs[0, 0].plot(x, y)
# axs[1, 1].scatter(x, y)
    