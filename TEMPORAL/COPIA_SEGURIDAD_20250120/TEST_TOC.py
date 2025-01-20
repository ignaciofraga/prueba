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
import psycopg2
from os import listdir
from os.path import isfile, join

# programa_muestreo = 'RADPROF'
# tipo_salida       = 'ANUAL'
# anho_salida       = 2023

# # Carga de informacion previa
# con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn             = create_engine(con_engine)
# tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
# tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
# tabla_datos      = psql.read_sql('SELECT * FROM datos_discretos', conn)
# tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
# tabla_variables  = psql.read_sql('SELECT * FROM variables_procesado', conn)
# conn.dispose() 


# archivo_estadillo_toc   = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADPROF/2023/Estadillo_TOC_RADPROF2023.xlsx'
# datos_estadillo         = pandas.read_excel(archivo_estadillo_toc)    
# datos_estadillo         = datos_estadillo.rename(columns={"Estación":'estacion',"Cast":'num_cast',"Niskin":'botella',"Frasco TOC":'vial_toc',
#                                                           "Lon":'longitud_muestreo',"Lat":'latitud_muestreo',"prebtl":'presion_ctd',"tembtl":'temperatura_ctd',"salbtl":'salinidad_ctd'})
      
# # Convierte las fechas de DATE a formato correcto
# datos_estadillo['fecha_muestreo'] = pandas.to_datetime(datos_estadillo['Date'], format='%Y%m%d').dt.date
# datos_estadillo['hora_muestreo']  = pandas.to_datetime(datos_estadillo['Date'], format='%Y%m%d').dt.time


# # # # Inserta en la base de datos
# # # conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# # # cursor = conn.cursor()                      
# # # instruccion_sql = 'DELETE FROM muestreos_discretos WHERE salida_mar = 3825;'        
# # # cursor.execute(instruccion_sql)
# # # conn.commit()
# # # cursor.close()
# # # conn.close()

# Recupera el identificador del programa de muestreo
#id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra la estación asociada a cada registro
# print('Asignando la estación correspondiente a cada medida')
# datos_estadillo = FUNCIONES_PROCESADO.evalua_estaciones(datos_estadillo,id_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_muestreos)

# # Encuentra las salidas al mar correspondientes 
# datos_estadillo = FUNCIONES_PROCESADO.evalua_salidas(datos_estadillo,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_salidas,tabla_muestreos)
 
# # Encuentra el identificador asociado a cada registro
# print('Asignando el registro correspondiente a cada medida')
# datos_estadillo = FUNCIONES_PROCESADO.evalua_registros(datos_estadillo,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)
   

# # Introduce los datos en la base de datos
# print('Introduciendo los datos en la base de datos')
# texto_insercion = FUNCIONES_PROCESADO.inserta_datos(datos_estadillo,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos,tabla_muestreos)



directorio_datos           = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/INSTRUMENTACION/TOC/Procesado/ESTADILLOS ANTIGUOS'

listado_archivos = [f for f in listdir(directorio_datos) if isfile(join(directorio_datos, f))]

for iarchivo in range(len(listado_archivos)):

   
    archivo_toc            = directorio_datos + '/' + listado_archivos[iarchivo]    

    # Lectura del archivo con los resultados del TOC
    datos_archivo              = pandas.read_excel(archivo_toc,skiprows=27)            
    
    # Mantén un recorte con las variables que interesan
    datos_toc = datos_archivo[['muestra','conc.','conc..1']]

    # Subset con los datos correspondientes a muestras
    datos_muestras = datos_toc[datos_toc[['muestra']].notna().all(axis=1)]
    
    # Metadatos
    datos_archivo_completo = pandas.read_excel(archivo_toc) 
    
    pte_carbono            = datos_archivo_completo.iloc[5].iloc[17]
    r2_carbono             = datos_archivo_completo.iloc[7].iloc[17]
    area_blanco_carbono    = datos_archivo_completo.iloc[3].iloc[18]
    conc_blanco_carbono    = datos_archivo_completo.iloc[3].iloc[19]
    
    pte_nitrogeno            = datos_archivo_completo.iloc[14].iloc[17]
    r2_nitrogeno             = datos_archivo_completo.iloc[16].iloc[17]
    area_blanco_nitrogeno    = datos_archivo_completo.iloc[12].iloc[18]
    conc_blanco_nitrogeno    = datos_archivo_completo.iloc[12].iloc[19]
    
    lcw_c = None
    lcw_n = None
    dsr_c = None
    dsr_n = None
    
    for idato in range(datos_archivo.shape[0]):
        if 'lcw' in str(datos_archivo['muestra'].iloc[idato]).lower():
            lcw_c = datos_archivo['conc.'].iloc[idato]
            lcw_n = datos_archivo['conc..1'].iloc[idato]
        if 'dsr' in str(datos_archivo['muestra'].iloc[idato]).lower():
            dsr_c = datos_archivo['conc.'].iloc[idato]
            dsr_n = datos_archivo['conc..1'].iloc[idato]
    
    fecha_analisis = datos_archivo_completo.iloc[4].iloc[4].date()
    
 
    print('Archivo ',listado_archivos[iarchivo], 'pte C',pte_carbono, 'lwc ',lcw_c)
    
    # Insercion en la pagina correspondiente
    instruccion_sql = '''INSERT INTO parametros_analisis_toc (fecha_analisis,pte_carbono,r2_carbono,area_blanco_carbono,conc_blanco_carbono,pte_nitrogeno,r2_nitrogeno,area_blanco_nitrogeno,conc_blanco_nitrogeno,lcw_c,lcw_n,dsr_c,dsr_n)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (fecha_analisis) DO UPDATE SET (pte_carbono,r2_carbono,area_blanco_carbono,conc_blanco_carbono,pte_nitrogeno,r2_nitrogeno,area_blanco_nitrogeno,conc_blanco_nitrogeno,lcw_c,lcw_n,dsr_c,dsr_n) = ROW(EXCLUDED.pte_carbono,EXCLUDED.r2_carbono,EXCLUDED.area_blanco_carbono,EXCLUDED.conc_blanco_carbono,EXCLUDED.pte_nitrogeno,EXCLUDED.r2_nitrogeno,EXCLUDED.area_blanco_nitrogeno,EXCLUDED.conc_blanco_nitrogeno,EXCLUDED.lcw_c,EXCLUDED.lcw_n,EXCLUDED.dsr_c,EXCLUDED.dsr_n);''' 
    
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()    
    cursor.execute(instruccion_sql,(fecha_analisis,pte_carbono,r2_carbono,area_blanco_carbono,conc_blanco_carbono,pte_nitrogeno,r2_nitrogeno,area_blanco_nitrogeno,conc_blanco_nitrogeno,lcw_c,lcw_n,dsr_c,dsr_n))
    conn.commit() 
    cursor.close()
    conn.close()







# archivo_toc            = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADPROF/2022/20231005_TOC_TN_RADPROF22_04.xlsx'

# # Lectura del archivo con los resultados del TOC
# datos_archivo              = pandas.read_excel(archivo_toc,skiprows=25)            

# # Mantén un recorte con las variables que interesan
# datos_toc = datos_archivo[['estacion','botella','muestra','conc C','conc N']]

# # Subset con los datos correspondientes a muestras
# datos_muestras = datos_toc[datos_toc[['estacion','botella']].notna().all(axis=1)]

# datos_muestras         = datos_muestras.rename(columns={"conc C":'carbono_organico_total','conc N':"nitrogeno_total"})
   

# str_salida = programa_muestreo + ' ' + str(anho_salida)
# id_salida  = tabla_salidas['id_salida'][tabla_salidas['nombre_salida']==str_salida].iloc[0]

# datos_muestras['salida_mar'] = id_salida

# # Encuentra el identificador asociado a cada registro
# print('Asignando el registro correspondiente a cada medida')
# datos_estadillo = FUNCIONES_PROCESADO.evalua_registros(datos_muestras,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)
   

# # Introduce los datos en la base de datos
# print('Introduciendo los datos en la base de datos')
# texto_insercion = FUNCIONES_PROCESADO.inserta_datos(datos_muestras,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos,tabla_muestreos)


# # Metadatos
# archivo_toc            = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADPROF/2022/20231005_TOC_TN_RADPROF22_04.xlsx'
# datos_archivo_completo = pandas.read_excel(archivo_toc) 

# pte_carbono            = datos_archivo_completo.iloc[5].iloc[19]
# r2_carbono             = datos_archivo_completo.iloc[7].iloc[19]
# area_blanco_carbono    = datos_archivo_completo.iloc[3].iloc[20]
# conc_blanco_carbono    = datos_archivo_completo.iloc[3].iloc[21]

# pte_nitrogeno            = datos_archivo_completo.iloc[14].iloc[19]
# r2_nitrogeno             = datos_archivo_completo.iloc[16].iloc[19]
# area_blanco_nitrogeno    = datos_archivo_completo.iloc[12].iloc[20]
# conc_blanco_nitrogeno    = datos_archivo_completo.iloc[12].iloc[21]

# for idato in range(datos_archivo.shape[0]):
#     if 'lcw' in str(datos_archivo['muestra'].iloc[idato]).lower():
#         lcw_c = datos_archivo['conc C'].iloc[idato]
#         lcw_n = datos_archivo['conc N'].iloc[idato]
#     if 'dsr' in str(datos_archivo['muestra'].iloc[idato]).lower():
#         dsr_c = datos_archivo['conc C'].iloc[idato]
#         dsr_n = datos_archivo['conc N'].iloc[idato]

# fecha_analisis = datos_archivo_completo.iloc[4].iloc[6].date()


# # # Insercion en la pagina correspondiente
# # instruccion_sql = '''INSERT INTO parametros_analisis_toc (fecha_analisis,pte_carbono,r2_carbono,area_blanco_carbono,conc_blanco_carbono,pte_nitrogeno,r2_nitrogeno,area_blanco_nitrogeno,conc_blanco_nitrogeno,lcw_c,lcw_n,dsr_c,dsr_n)
# # VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (fecha_analisis) DO UPDATE SET (pte_carbono,r2_carbono,area_blanco_carbono,conc_blanco_carbono,pte_nitrogeno,r2_nitrogeno,area_blanco_nitrogeno,conc_blanco_nitrogeno,lcw_c,lcw_n,dsr_c,dsr_n) = ROW(EXCLUDED.pte_carbono,EXCLUDED.r2_carbono,EXCLUDED.area_blanco_carbono,EXCLUDED.conc_blanco_carbono,EXCLUDED.pte_nitrogeno,EXCLUDED.r2_nitrogeno,EXCLUDED.area_blanco_nitrogeno,EXCLUDED.conc_blanco_nitrogeno,EXCLUDED.lcw_c,EXCLUDED.lcw_n,EXCLUDED.dsr_c,EXCLUDED.dsr_n);''' 

# # conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# # cursor = conn.cursor()    
# # cursor.execute(instruccion_sql,(fecha_analisis,pte_carbono,r2_carbono,area_blanco_carbono,conc_blanco_carbono,pte_nitrogeno,r2_nitrogeno,area_blanco_nitrogeno,conc_blanco_nitrogeno,lcw_c,lcw_n,dsr_c,dsr_n))
# # conn.commit() 
# # cursor.close()
# # conn.close()


    # # Genera la intrucción de escritura
    # str_var = ','.join(listado_adicional)
    # str_com = ','.join(listado_variables_comunes)
    # listado_exc = ['EXCLUDED.' + s for s in listado_variables_comunes]
    # str_exc = ','.join(listado_exc)
    # listado_str = ['%s']*len(listado_adicional)
    # str_car = ','.join(listado_str)
    
    # instruccion_sql = 'INSERT INTO ' + tabla_destino + '(' + str_var + ') VALUES (' + str_car + ') ON CONFLICT (' + puntero + ') DO UPDATE SET (' + str_com + ') = ROW(' + str_exc +');'
      



    #fecha_analisis,pte_carbono,r2_carbono,area_blanco_carbono,conc_blanco_carbono,pte_nitrogeno,r2_nitrogeno,area_blanco_nitrogeno,conc_blanco_nitrogeno,lcw_c,lcw_n,dsr_c,dsr_n
    # instruccion_sql = 'INSERT INTO ' + tabla_destino + '(' + str_var + ') VALUES (' + str_car + ') ON CONFLICT (' + puntero + ') DO UPDATE SET (' + str_com + ') = ROW(' + str_exc +');'



# #archivo_resultados   = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/PRUEBAS STREAMLIT/RADCOR_2022.xlsm'
# #datos_res            = pandas.read_excel(archivo_resultados,sheet_name='DatosFinales',skiprows=1) 



# rendimiento_columna     = 100
# temperatura_laboratorio = 19.9


# con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn_psql        = create_engine(con_engine)
# df_rmns_bajos    = psql.read_sql('SELECT * FROM rmn_bajo_nutrientes', conn_psql)
# df_rmns_altos    = psql.read_sql('SELECT * FROM rmn_alto_nutrientes', conn_psql)
# df_muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
# df_estaciones             = psql.read_sql('SELECT * FROM estaciones', conn_psql)
# df_datos_biogeoquimicos   = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn_psql)
# df_datos_fisicos          = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn_psql)

# conn_psql.dispose()   

# df_referencias_altas = df_rmns_altos[df_rmns_altos['nombre_rmn']=='CH']
# df_referencias_bajas = df_rmns_bajos[df_rmns_bajos['nombre_rmn']=='CE']

# # Define los vectores con las variables a procesar
# variables_procesado    = ['TON','Nitrato','Nitrito','Silicato','Fosfato']    
# variables_procesado_bd = ['ton','nitrato','nitrito','silicato','fosfato']
# variables_unidades     = ['\u03BCmol/kg','\u03BCmol/kg','\u03BCmol/kg','\u03BCmol/kg','\u03BCmol/kg']
# canales_autoanalizador = ['ton','nitrito','silicato','fosfato']


# # Lectura del archivo con los resultados del AA
# datos_AA              = pandas.read_excel(archivo_AA,skiprows=15)            
# datos_AA              = datos_AA.rename(columns={"Results 1":canales_autoanalizador[0],"Results 2":canales_autoanalizador[1],"Results 3":canales_autoanalizador[2],"Results 4":canales_autoanalizador[3]})
      
# # Identifica qué canales/variables se han procesado
# variables_procesadas = datos_AA.columns.tolist()
# variables_run        = list(set(variables_procesadas).intersection(variables_procesado_bd))

# # Añade la información de salinidad en aquellas muestras que tienen un muestreo asociado                                            
# df_datos_disponibles  = pandas.merge(df_datos_fisicos, df_muestreos, on="muestreo")            

# # Adapta el nombre de las sw
# for idato in range(datos_AA.shape[0]):
#     if datos_AA['Sample ID'].iloc[idato][0:2].lower()=='sw':
#        datos_AA['Sample ID'].iloc[idato] ='sw' 
        
# # Encuentra las posiciones de las referencias de sw
# indices_referencias = numpy.asarray(datos_AA['Peak Number'][datos_AA['Sample ID']=='sw']) - 1
# # Agrupa en dos tandas, las iniciales y las finales
# spl          = [0]+[i for i in range(1,len(indices_referencias)) if indices_referencias[i]-indices_referencias[i-1]>1]+[None]
# listado_refs = [indices_referencias[b:e] for (b, e) in [(spl[i-1],spl[i]) for i in range(1,len(spl))]]

# ref_inicial        = listado_refs[0][-1] + 1
# ref_final          = listado_refs[1][0]

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
#     datos_corregidos = FUNCIONES_PROCESADO.correccion_drift(datos_AA,df_referencias_altas,df_referencias_bajas,variables_run,rendimiento_columna,temperatura_laboratorio)
# #     datos_entrada = datos_AA                
    
    
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
    