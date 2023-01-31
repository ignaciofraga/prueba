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


import FUNCIONES_PROCESADO
import numpy


import seawater


archivo_AA   = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/211129_RCAN19_ENE_costR1R1.xlsx'
archivo_AA   = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/221003_RCAN19_MAR_costR1R1.xlsx'

archivo_refs = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/REFERENCIAS_RADCAN19.xlsx'

archivo_datos_procesados = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/DATOS_PROCESADOS.xlsx'


rendimiento_columna     = 100
temperatura_laboratorio = 21.5

# Variables
variables_run = ['ton','nitrito','silicato','fosfato'] 

# Lectura del archivo con las referencias
df_referencias        = pandas.read_excel(archivo_refs)   

# Lectura del archivo con los resultados del AA
datos_AA              = pandas.read_excel(archivo_AA,skiprows=15)            
datos_AA              = datos_AA.rename(columns={"Results 1":variables_run[0],"Results 2":variables_run[1],"Results 3":variables_run[2],"Results 4":variables_run[3]})
      
# Añade la información de salinidad en aquellas muestras que tienen un muestreo asociado
###### PARTE DIFERENTE CON EL STREAMLIT ######
# df_muestreos          = df_muestreos.rename(columns={"id_muestreo":"muestreo"}) # Para igualar los nombres de columnas                                               
# df_datos_disponibles  = pandas.merge(df_datos_fisicos, df_muestreos, on="muestreo")            

df_datos_disponibles         = pandas.read_excel(archivo_datos_procesados,skiprows=1)            
df_datos_disponibles         = df_datos_disponibles.rename(columns={'salbtl':'salinidad_ctd','ID':'nombre_muestreo'})


indices_dataframe        = numpy.arange(0,df_datos_disponibles.shape[0],1,dtype=int)    
df_datos_disponibles['muestreo'] = indices_dataframe
df_datos_disponibles.set_index('muestreo',drop=False,append=False,inplace=True)

####### FIN DE LA PARTE DIFERENTE AL STREAMLIT #######
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
    texto_error = "Ninguna de las muestras analizadas se corresponde con muestreos incluidos en la base de datos"
    #st.warning(texto_error, icon="⚠️")          
   
else:
    
# En caso contrario procesa los datos

    # # Aplica la corrección de deriva (DRIFT)                 
#    datos_corregidos = FUNCIONES_PROCESADO.correccion_drift(datos_AA,df_referencias,variables_run,rendimiento_columna,temperatura_laboratorio)




    datos_entrada = datos_AA

    # Predimensiona un dataframe con los resultados de la correccion
    datos_corregidos = pandas.DataFrame(columns=variables_run)    

    # Encuentra los índices (picos) correspondientes a la calbración
    indices_calibracion = numpy.asarray(datos_entrada['Peak Number'][datos_entrada['Cup Type']=='CALB']) - 1
           
    # Corrige las concentraciones a partir de los rendimientos de la coumna reductora
    datos_entrada['nitrato_rendimiento'] = numpy.zeros(datos_entrada.shape[0])
    datos_entrada['ton_rendimiento'] = numpy.zeros(datos_entrada.shape[0])
    factor = ((datos_entrada['ton'].iloc[indices_calibracion[-1]]*rendimiento_columna/100) + datos_entrada['nitrito'].iloc[indices_calibracion[-1]])/(datos_entrada['ton'].iloc[indices_calibracion[-1]] + datos_entrada['nitrito'].iloc[indices_calibracion[-1]])
    for idato in range(datos_entrada.shape[0]):
        datos_entrada['nitrato_rendimiento'].iloc[idato] = (datos_entrada['ton'].iloc[idato]*factor - datos_entrada['nitrito'].iloc[idato])/(rendimiento_columna/100) 
        datos_entrada['ton_rendimiento'].iloc[idato] = datos_entrada['nitrato_rendimiento'].iloc[idato] + datos_entrada['nitrito'].iloc[idato]
    
    # Asocia la temperatura de laboratorio a todas las muestras
    datos_entrada['temp.lab'] = temperatura_laboratorio
    
    # Pasa las concentraciones a mol/kg
    posicion_RMN_bajos = numpy.zeros(2,dtype=int)
    posicion_RMN_altos = numpy.zeros(2,dtype=int)
    icont_bajos        = 0
    icont_altos        = 0

    for idato in range(datos_entrada.shape[0]):

        if datos_entrada['Sample ID'].iloc[idato][0:7].lower() == 'rmn low' :
            posicion_RMN_bajos[icont_bajos] = idato
            icont_bajos                     = icont_bajos + 1 
            datos_entrada['salinidad'].iloc[idato]  = df_referencias['Sal'][0]
            
        if datos_entrada['Sample ID'].iloc[idato][0:8].lower() == 'rmn high':
            posicion_RMN_altos[icont_altos] = idato
            icont_altos                     = icont_altos + 1
            datos_entrada['salinidad'].iloc[idato]  = df_referencias['Sal'][1]
            
    
    #densidades =seawater.eos80.pden(datos_entrada['salinidad'], datos_entrada['temp.lab'], p, pr=0)          
    densidades = seawater.eos80.dens0(datos_entrada['salinidad'], datos_entrada['temp.lab'])
    datos_entrada['DENSIDAD'] = densidades/1000  
    
    datos_entrada['ton_CONC'] = datos_entrada['ton_rendimiento']/datos_entrada['DENSIDAD']  
    datos_entrada['nitrato_CONC'] = datos_entrada['nitrato_rendimiento']/datos_entrada['DENSIDAD']  
    datos_entrada['nitrito_CONC'] = datos_entrada['nitrito']/datos_entrada['DENSIDAD']  
    datos_entrada['silicato_CONC'] = datos_entrada['silicato']/datos_entrada['DENSIDAD']  
    datos_entrada['fosfato_CONC'] = datos_entrada['fosfato']/datos_entrada['DENSIDAD']  
    
    
    ####  APLICA LA CORRECCIÓN DE DERIVA ####
    # Encuentra las posiciones de los RMNs
    #posicion_RMN_bajos  = [i for i, e in enumerate(datos_entrada['Sample ID']) if e == 'RMN Low']
    #posicion_RMN_altos  = [i for i, e in enumerate(datos_entrada['Sample ID']) if e == 'RMN High']
    
    #for ivariable in range(1):
    for ivariable in range(len(variables_run)):
               
        variable_concentracion  = variables_run[ivariable] + '_CONC'
        
        # Concentraciones de las referencias
        RMN_CE_variable = df_referencias[variables_run[ivariable]].iloc[0]
        RMN_CI_variable = df_referencias[variables_run[ivariable]].iloc[1]  
        
        # Concentraciones de las muestras analizadas como referencias
        RMN_altos       = datos_entrada[variable_concentracion][posicion_RMN_altos]
        RMN_bajos       = datos_entrada[variable_concentracion][posicion_RMN_bajos]
            
        # Predimensiona las rectas a y b
        posiciones_corr_drift = numpy.arange(posicion_RMN_altos[0]-1,posicion_RMN_bajos[1]+1)
        recta_at              = numpy.zeros(datos_entrada.shape[0])
        recta_bt              = numpy.zeros(datos_entrada.shape[0])
        
        store = numpy.zeros(datos_entrada.shape[0])
        store2 = numpy.zeros(datos_entrada.shape[0])
    
        pte_RMN      = (RMN_CI_variable-RMN_CE_variable)/(RMN_altos.iloc[0]-RMN_bajos.iloc[0]) 
        t_indep_RMN  = RMN_CE_variable- pte_RMN*RMN_bajos.iloc[0] 
    
        variable_drift = numpy.zeros(datos_entrada.shape[0])
    
        # Aplica la corrección basada de dos rectas, descrita en Hassenmueller
        for idato in range(posiciones_corr_drift[0],posiciones_corr_drift[-1]):
            factor_f        = (idato-posiciones_corr_drift[0])/(posiciones_corr_drift[-1]-posiciones_corr_drift[0])
            store[idato]    = factor_f
            # recta_at[idato] = RMN_bajos.iloc[0] +  factor_f*(RMN_bajos.iloc[0]-RMN_bajos.iloc[-1]) 
            # recta_bt[idato] = RMN_altos.iloc[0] -  factor_f*(RMN_altos.iloc[0]-RMN_altos.iloc[-1]) 
            recta_at[idato] = RMN_bajos.iloc[0] +  factor_f*(RMN_bajos.iloc[-1]-RMN_bajos.iloc[0]) 
            recta_bt[idato] = RMN_altos.iloc[0] +  factor_f*(RMN_altos.iloc[-1]-RMN_altos.iloc[0]) 
    
            val_combinado         = ((datos_entrada[variable_concentracion][idato]-recta_at[idato])/(recta_bt[idato]-recta_at[idato]))*(RMN_altos.iloc[0]-RMN_bajos.iloc[0]) + RMN_bajos.iloc[0]
            store2[idato] = val_combinado
    
            variable_drift[idato] = val_combinado*pte_RMN+t_indep_RMN
    
        variable_drift[variable_drift<0] = 0
        
        datos_corregidos[variables_run[ivariable]] = variable_drift
        
    # Añade columna con el identificador de cada muestra
    datos_corregidos['nombre_muestreo'] = datos_entrada['Sample ID']







    # COMPARA CON PROCESADO "TRADICIONAL"
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
    