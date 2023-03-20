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

#from sklearn.metrics import r2_score
import scipy
import seawater


archivo_AA      = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/221005_RCAN19_ABR_costR1R1.xlsx'
mes_comparacion = 'ABRIL'

archivo_datos_procesados = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/RCAN2019cost_revisar.xlsx'
directorio_resultados    = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/COMPARACION_PROCESADO'

# rendimiento_columna     = 100
# temperatura_laboratorio = 21.3
# mes_comparacion = 'ABRIL'

rmn_elegida             = 'RADCAN COSTERAS 2019'
abreviatura_programa    = 'RADCAN'

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

listado_meses = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPT-OCT']
listado_temp  = [16.8,17.8,21.5,21.3,19.5,21.4,21.1,22.8,21.4]
listado_rtos  = [100,95.12,100,100,100,95.22,94.94,95.42,96.02]

#for imes in range(len(listado_meses)):
for imes in range(3,4):

    archivo_AA = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/' + listado_meses[imes] + '.xlsx'
    rendimiento_columna     = listado_rtos[imes]
    temperatura_laboratorio = listado_temp[imes]
    mes_comparacion = listado_meses[imes]
    
    # Lectura del archivo con los resultados del AA
    datos_AA              = pandas.read_excel(archivo_AA,skiprows=15)            
    datos_AA              = datos_AA.rename(columns={"Results 1":variables_run[0],"Results 2":variables_run[1],"Results 3":variables_run[2],"Results 4":variables_run[3]})
          
    
    # CAMBIA NOMBRE DE LAS MUESTRAS PARA ADAPTARLO A NOMENCLATURA STREAMLIT
    archivo_metadatos       = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/DATOS_FISICOS_RADCAN2019.xlsx'
    datos_temp              = pandas.read_excel(archivo_metadatos)  
    
    for idato_aa in range(datos_AA.shape[0]):
        
        for idato_ref in range(datos_temp.shape[0]):
            if datos_AA['Sample ID'].iloc[idato_aa] == datos_temp['ID'].iloc[idato_ref]:
                
                nombre_estacion                              = datos_temp['estacion'].iloc[idato_ref]
                
                nombre_muestreo     = abreviatura_programa + '_' + datos_temp['fecha_muestreo'].iloc[idato_ref].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
                if datos_temp['num_cast'].iloc[idato_ref] is not None:
                    nombre_muestreo = nombre_muestreo + '_C' + str(round(datos_temp['num_cast'].iloc[idato_ref]))
                else:
                    nombre_muestreo = nombre_muestreo + '_C1' 
                    
                if datos_temp['botella'].iloc[idato_ref] is not None:
                    nombre_muestreo = nombre_muestreo + '_B' + str(round(datos_temp['botella'].iloc[idato_ref])) 
                else:
                    if datos_temp['prof_referencia'].iloc[idato_ref] is not None: 
                        nombre_muestreo = nombre_muestreo + '_P' + str(round(datos_temp['prof_referencia'].iloc[idato_ref]))
                    else:
                        nombre_muestreo = nombre_muestreo + '_P' + str(round(datos_temp['presion_ctd'].iloc[idato_ref])) 
                                
                datos_AA['Sample ID'].iloc[idato_aa] = nombre_muestreo
                
    # FIN DEL CAMBIO DE NOMBRE            
    
    
    
    
    
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
        df_datos_disponibles         = pandas.read_excel(archivo_datos_procesados,sheet_name='Muestras_CorregRMN', skiprows=1)            
        df_datos_disponibles         = df_datos_disponibles.rename(columns={'salbtl':'salinidad_ctd','ID':'nombre_muestreo'})
    
        # CAMBIA NOMBRE DE LAS MUESTRAS PARA ADAPTARLO A NOMENCLATURA STREAMLIT
        archivo_metadatos       = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/DATOS_FISICOS_RADCAN2019.xlsx'
        datos_temp              = pandas.read_excel(archivo_metadatos)  
        
        for idato_aa in range(df_datos_disponibles.shape[0]):
            
            for idato_ref in range(datos_temp.shape[0]):
                if df_datos_disponibles['nombre_muestreo'].iloc[idato_aa] == datos_temp['ID'].iloc[idato_ref]:
                    
                    nombre_estacion                              = datos_temp['estacion'].iloc[idato_ref]
                    
                    nombre_muestreo     = abreviatura_programa + '_' + datos_temp['fecha_muestreo'].iloc[idato_ref].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
                    if datos_temp['num_cast'].iloc[idato_ref] is not None:
                        nombre_muestreo = nombre_muestreo + '_C' + str(round(datos_temp['num_cast'].iloc[idato_ref]))
                    else:
                        nombre_muestreo = nombre_muestreo + '_C1' 
                        
                    if datos_temp['botella'].iloc[idato_ref] is not None:
                        nombre_muestreo = nombre_muestreo + '_B' + str(round(datos_temp['botella'].iloc[idato_ref])) 
                    else:
                        if datos_temp['prof_referencia'].iloc[idato_ref] is not None: 
                            nombre_muestreo = nombre_muestreo + '_P' + str(round(datos_temp['prof_referencia'].iloc[idato_ref]))
                        else:
                            nombre_muestreo = nombre_muestreo + '_P' + str(round(datos_temp['presion_ctd'].iloc[idato_ref])) 
                                    
                    df_datos_disponibles['nombre_muestreo'].iloc[idato_aa] = nombre_muestreo
                    
        # FIN DEL CAMBIO DE NOMBRE            
    
    
    
    
    
        indices_dataframe        = numpy.arange(0,df_datos_disponibles.shape[0],1,dtype=int)    
        df_datos_disponibles['muestreo'] = indices_dataframe
        df_datos_disponibles.set_index('muestreo',drop=False,append=False,inplace=True)
        
        
        df_datos_disponibles  = df_datos_disponibles.rename(columns={'NO3+NO2.1':'ton_proc','NO2.1':'nitrito_proc','SiO2.1':'silicato_proc','PO4.1':'fosfato_proc'})
        df_datos_disponibles_comparacion  = df_datos_disponibles[['ton_proc','nitrito_proc','silicato_proc','fosfato_proc','nombre_muestreo']]
    
    
        tabla_comparacion = pandas.merge(df_datos_disponibles_comparacion, datos_corregidos, on="nombre_muestreo")   
    
        tabla_comparacion[tabla_comparacion['ton_proc']<0]=0
        tabla_comparacion[tabla_comparacion['nitrito_proc']<0]=0
    
        f, axs = plt.subplots(2, 2)
        axs[0, 0].scatter(tabla_comparacion['ton'], tabla_comparacion['ton_proc'],marker='.')
        axs[0, 0].plot([min(tabla_comparacion['ton']),max(tabla_comparacion['ton'])], [min(tabla_comparacion['ton']),max(tabla_comparacion['ton'])])
        axs[0, 0].set_title('TON')
        #
        res  = scipy.stats.linregress(tabla_comparacion['ton'], tabla_comparacion['ton_proc'])
        r2_ton = res.rvalue**2
        texto_r2 = 'R2=' + str(round(r2_ton,4))
        axs[0, 0].text(0.1, 0.9, texto_r2,ha='left', va='top', transform=axs[0, 0].transAxes)
    
    
        axs[0, 1].scatter(tabla_comparacion['nitrito'], tabla_comparacion['nitrito_proc'],marker='.')
        axs[0, 1].plot([min(tabla_comparacion['nitrito']),max(tabla_comparacion['nitrito'])], [min(tabla_comparacion['nitrito']),max(tabla_comparacion['nitrito'])])
        axs[0, 1].set_title('NITRITO')
        #
        res  = scipy.stats.linregress(tabla_comparacion['nitrito'], tabla_comparacion['nitrito_proc'])
        r2_nit = res.rvalue**2
        texto_r2 = 'R2=' + str(round(r2_nit,4))
        axs[0, 1].text(0.1, 0.9, texto_r2,ha='left', va='top', transform=axs[0, 1].transAxes)
        
        
        axs[1, 0].scatter(tabla_comparacion['silicato'], tabla_comparacion['silicato_proc'],marker='.')
        axs[1, 0].plot([min(tabla_comparacion['silicato']),max(tabla_comparacion['silicato'])], [min(tabla_comparacion['silicato']),max(tabla_comparacion['silicato'])])
        axs[1, 0].set_title('SILICATO')
        #
        res  = scipy.stats.linregress(tabla_comparacion['silicato'], tabla_comparacion['silicato_proc'])
        r2 = res.rvalue**2
        texto_r2 = 'R2=' + str(round(r2,4))
        axs[1, 0].text(0.1, 0.9, texto_r2,ha='left', va='top', transform=axs[1, 0].transAxes)
        
        
        axs[1, 1].scatter(tabla_comparacion['fosfato'], tabla_comparacion['fosfato_proc'],marker='.')
        axs[1, 1].plot([min(tabla_comparacion['fosfato']),max(tabla_comparacion['fosfato'])], [min(tabla_comparacion['fosfato']),max(tabla_comparacion['fosfato'])])
        axs[1, 1].set_title('FOSFATO')  
        #
        res  = scipy.stats.linregress(tabla_comparacion['fosfato'], tabla_comparacion['fosfato'])
        r2 = res.rvalue**2
        texto_r2 = 'R2=' + str(round(r2,4))
        axs[1, 1].text(0.1, 0.9, texto_r2,ha='left', va='top', transform=axs[1, 1].transAxes)
        
      
        plt.subplots_adjust(left=0.1,bottom=0.1,right=0.9,top=0.9,wspace=0.4,hspace=0.4) 
        texto_titulo = 'Comparacion mes ' + mes_comparacion
        f.suptitle(texto_titulo, fontsize=14)
        
        archivo_resultados = directorio_resultados + '/' + mes_comparacion + '.png'
        f.savefig(archivo_resultados, bbox_inches='tight')
        
#     fig, axs = plt.subplots(2, 2, subplot_kw=dict(projection="polar"))
# axs[0, 0].plot(x, y)
# axs[1, 1].scatter(x, y)
    