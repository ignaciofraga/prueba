# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 08:27:43 2023

@author: ifraga
"""
import numpy
import pandas
import datetime
from pyproj import Proj
import math
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine
import json
import seawater
import FUNCIONES_LECTURA
import FUNCIONES_PROCESADO
import pandas
pandas.options.mode.chained_assignment = None
import datetime


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

archivo_datos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADCAN/DATOS_FISICOS_RADCAN2019.xlsx'

tipo_salida = 'MENSUAL'

programa_seleccionado = 'RADIAL CANTABRICO'



df_datos_importacion  = pandas.read_excel(archivo_datos) 


# corrige el formato de las fechas
for idato in range(df_datos_importacion.shape[0]):
    df_datos_importacion['fecha_muestreo'][idato] = (df_datos_importacion['fecha_muestreo'][idato]).date()           
    if df_datos_importacion['fecha_muestreo'][idato]:
        if isinstance(df_datos_importacion['hora_muestreo'][idato], str):
            df_datos_importacion['hora_muestreo'][idato] = datetime.datetime.strptime(df_datos_importacion['hora_muestreo'][idato], '%H:%M:%S').time()




# Realiza un control de calidad primario a los datos importados   
datos_corregidos,textos_aviso   = FUNCIONES_PROCESADO.control_calidad(df_datos_importacion,direccion_host,base_datos,usuario,contrasena,puerto)  

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)


# Encuentra la estación asociada a cada registro
datos_corregidos = FUNCIONES_PROCESADO.evalua_estaciones(datos_corregidos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# Encuentra las salidas al mar correspondientes  
datos_corregidos = FUNCIONES_PROCESADO.evalua_salidas(datos_corregidos,id_programa,programa_seleccionado,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
 

# Encuentra el identificador asociado a cada registro
datos_corregidos = FUNCIONES_PROCESADO.evalua_registros(datos_corregidos,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
 

# datos = datos_corregidos

# # Recupera la tabla con los registros de muestreos físicos
# con_engine                = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn_psql                 = create_engine(con_engine)
# tabla_registros_fisica    = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn_psql)

# # Genera un dataframe solo con las variales fisicas de los datos a importar 
# datos_fisica = datos[['temperatura_ctd', 'temperatura_ctd_qf','salinidad_ctd','salinidad_ctd_qf','par_ctd','par_ctd_qf','turbidez_ctd','turbidez_ctd_qf','id_muestreo']]
# datos_fisica = datos_fisica.rename(columns={"id_muestreo": "muestreo"})

# # # Si no existe ningún registro en la base de datos, introducir todos los datos disponibles
# if tabla_registros_fisica.shape[0] == 0:
#     datos_fisica.set_index('muestreo',drop=True,append=False,inplace=True)
#     datos_fisica.to_sql('datos_discretos_fisica', conn_psql,if_exists='append')
    

# # En caso contrario, comprobar qué parte de la información está en la base de datos
# else: 
#     # Elimina, en el dataframe con los datos de la base de datos, los registros que ya están en los datos a importar
#     for idato in range(datos_fisica.shape[0]):
#         #try:
#         tabla_registros_fisica = tabla_registros_fisica.drop(tabla_registros_fisica[tabla_registros_fisica.muestreo == datos_fisica['muestreo'][idato]].index)
#         # except:
#         #     pass
        
#     # Une ambos dataframes, el que contiene los datos nuevo y el que tiene los datos que ya están en la base de datos
#     datos_conjuntos = pandas.concat([tabla_registros_fisica, datos_fisica])
        
#     vector_identificadores            = numpy.arange(1,datos_conjuntos.shape[0]+1)    
#     datos_conjuntos['muestreo'] = vector_identificadores
    
#     datos_conjuntos.set_index('muestreo',drop=True,append=False,inplace=True)
    
#     # borra los registros existentes en la tabla (no la tabla en sí, para no perder tipos de datos y referencias)
#     conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
#     cursor = conn.cursor()
#     instruccion_sql = "TRUNCATE datos_discretos_fisica;"
#     cursor.execute(instruccion_sql)
#     conn.commit()
#     cursor.close()
#     conn.close() 
    
#     # Inserta el dataframe resultante en la base de datos 
#     datos_conjuntos.to_sql('datos_discretos_fisica', conn_psql,if_exists='append')


# conn_psql.dispose() # Cierra la conexión con la base de datos 



  

#FUNCIONES_PROCESADO.inserta_datos_fisica(datos_corregidos,direccion_host,base_datos,usuario,contrasena,puerto)

# datos = datos_corregidos


# # Recupera la tabla con los registros de los muestreos
# con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn_psql        = create_engine(con_engine)
# tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
# tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
   
# datos['id_muestreo_temp']  = numpy.zeros(datos.shape[0],dtype=int)

# # si no hay ningun valor en la tabla de registro, meter directamente todos los datos registrados
# if tabla_muestreos.shape[0] == 0:

#     # genera un dataframe con las variables que interesa introducir en la base de datos
#     exporta_registros                    = datos[['id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','configuracion_perfilador','configuracion_superficie','latitud','longitud']]
#     # añade el indice de cada registro
#     indices_registros                    = numpy.arange(1,(exporta_registros.shape[0]+1))    
#     exporta_registros['id_muestreo']     = indices_registros
#     # renombra la columna con información de la estación muestreada
#     exporta_registros                    = exporta_registros.rename(columns={"id_estacion_temp":"estacion",'id_salida':'salida_mar','latitud':'latitud_muestreo','longitud':'longitud_muestreo'})
#     # # añade el nombre del muestreo
#     exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
#     for idato in range(exporta_registros.shape[0]):    
#         nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'][idato]]['nombre_estacion'].iloc[0]
        
#         nombre_muestreo     = abreviatura_programa + '_' + datos['fecha_muestreo'][idato].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
#         if datos['num_cast'][idato] is not None:
#             nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'][idato]))
#         if datos['botella'][idato] is not None:
#             nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'][idato]))                
            
#         exporta_registros['nombre_muestreo'][idato]  = nombre_muestreo

#         datos['id_muestreo_temp'] [idato]            = idato + 1
        
        
#     # Inserta en base de datos        
#     exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
#     exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append') 

# # En caso contrario hay que ver registro a registro, si ya está incluido en la base de datos
# else:

#     ultimo_registro_bd         = max(tabla_muestreos['id_muestreo'])
#     datos['io_nuevo_muestreo'] = numpy.ones(datos.shape[0],dtype=int)

#     for idato in range(datos.shape[0]):

#         if datos['hora_muestreo'][idato] is not None:          
# #            df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'][idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'][idato]) & (tabla_muestreos['hora_muestreo']==datos['hora_muestreo'][idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'][idato]) &  (tabla_muestreos['configuracion_perfilador'] == datos['configuracion_perfilador'][idato]) & (tabla_muestreos['configuracion_superficie'] == datos['configuracion_superficie'][idato])]
#             df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'][idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'][idato]) & (tabla_muestreos['hora_muestreo']==datos['hora_muestreo'][idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'][idato])]

#         else:
#             # df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'][idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'][idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'][idato]) &  (tabla_muestreos['configuracion_perfilador'] == datos['configuracion_perfilador'][idato]) & (tabla_muestreos['configuracion_superficie'] == datos['configuracion_superficie'][idato])]
#             df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'][idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'][idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'][idato])]
              
#         if df_temp.shape[0]> 0:
#             datos['id_muestreo_temp'] [idato] =  df_temp['id_muestreo'].iloc[0]    
#             datos['io_nuevo_muestreo'][idato] = 0
            
#         else:
#             datos['io_nuevo_muestreo'][idato] = 1
#             ultimo_registro_bd                = ultimo_registro_bd + 1
#             datos['id_muestreo_temp'][idato]  = ultimo_registro_bd 
        
    
#     if numpy.count_nonzero(datos['io_nuevo_muestreo']) > 0:
    
#         # Genera un dataframe sólo con los valores nuevos, a incluir (io_nuevo_muestreo = 1)
#         nuevos_muestreos  = datos[datos['io_nuevo_muestreo']==1]
#         # Mantén sólo las columnas que interesan
#         exporta_registros = nuevos_muestreos[['id_muestreo_temp','id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','configuracion_perfilador','configuracion_superficie','latitud','longitud']]
                    
#         # Cambia el nombre de la columna de estaciones
#         exporta_registros = exporta_registros.rename(columns={"id_estacion_temp":"estacion","id_muestreo_temp":"id_muestreo",'id_salida':'salida_mar','latitud':'latitud_muestreo','longitud':'longitud_muestreo'})
#         # Indice temporal
#         exporta_registros['indice_temporal'] = numpy.arange(0,exporta_registros.shape[0])
#         exporta_registros.set_index('indice_temporal',drop=True,append=False,inplace=True)
#         # Añade el nombre del muestreo
#         exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
#         for idato in range(exporta_registros.shape[0]):    
#             nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'][idato]]['nombre_estacion'].iloc[0]
          
#             nombre_muestreo     = abreviatura_programa + '_' + datos['fecha_muestreo'][idato].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
#             if datos['num_cast'][idato] is not None:
#                 nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'][idato]))
#             if datos['botella'][idato] is not None:
#                 nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'][idato]))                
             
#             exporta_registros['nombre_muestreo'][idato]  = nombre_muestreo


#         # # Inserta el dataframe resultante en la base de datos 
#         exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
#         exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append') 




