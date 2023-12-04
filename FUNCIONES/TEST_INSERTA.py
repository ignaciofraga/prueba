# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""

import FUNCIONES_LECTURA
import FUNCIONES_PROCESADO
import FUNCIONES_AUXILIARES
import pandas
pandas.options.mode.chained_assignment = None
import datetime
import pandas.io.sql as psql
from sqlalchemy import create_engine
import psycopg2
import pickle

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_muestreo = 'RADPROF'
tipo_salida       = 'ANUAL'


con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn             = create_engine(con_engine)
tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
tabla_datos      = psql.read_sql('SELECT * FROM datos_discretos', conn)
tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
tabla_variables  = psql.read_sql('SELECT * FROM variables_procesado', conn)
conn.dispose() 


# # Rutas de los archivos a importar  
# #archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx' 
# archivo_datos                ='C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADPROF/2022/RADPROF_2022_BTL_03032023_AB_FORMATO.xlsx'

# # Importa el .xlsx
# df_datos_importacion = pandas.read_excel(archivo_datos,index_col=None)


# # Realiza un control de calidad primario a los datos importados   
# datos_corregidos,textos_aviso   = FUNCIONES_PROCESADO.control_calidad(df_datos_importacion)  

# # # Convierte las fechas de DATE a formato correcto
# # datos_corregidos['fecha_muestreo'] =  pandas.to_datetime(datos_corregidos['fecha_muestreo'], format='%d%m%Y').dt.date


# # Recupera el identificador del programa de muestreo
# id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)
        
# datos_corregidos['prof_referencia'] = None

# datos_corregidos['prof_referencia'] = round(datos_corregidos['presion_ctd']/5)*5


# listado_variables = datos_corregidos.columns.values.tolist()

# df_acc = pandas.DataFrame(columns=listado_variables)

# listado_variables_promedio = datos_corregidos.columns.values.tolist() 
# if 'fecha_muestreo' in listado_variables_promedio:
#     listado_variables_promedio.remove('fecha_muestreo')
# if 'hora_muestreo' in listado_variables_promedio:
#     listado_variables_promedio.remove('hora_muestreo')


# df_acc['promediado'] = None

# listado_estaciones = datos_corregidos['estacion'].unique()

# for iestacion in range(len(listado_estaciones)):
    
#     df_estacion   = datos_corregidos[datos_corregidos['estacion']==listado_estaciones[iestacion]]

#     listado_casts = df_estacion['num_cast'].unique()
    
#     for icast in range(len(listado_casts)):    

#         df_cast      = df_estacion[df_estacion['num_cast']==listado_casts[icast]]
       
#         profs_unicas = df_cast['prof_referencia'].unique()
        
#         for iprof_unica in range(len(profs_unicas)):
    
#             datos_prof = df_cast[df_cast['prof_referencia']==profs_unicas[iprof_unica]] 
            
#             if datos_prof.shape[0]>1:
            
#                 #promedios = datos_prof.mean(axis=0)
                
#                 promedios = datos_prof[listado_variables_promedio].mean()
                
#                 df_promedio = pandas.DataFrame([promedios])
                
#                 df_promedio['fecha_muestreo'] = datos_prof['fecha_muestreo'].iloc[0]
#                 df_promedio['promediado']     = 1
            
#                 df_acc = pandas.concat([df_acc, df_promedio])
            
#             else:
        
#                 df_acc = pandas.concat([df_acc, datos_prof])


#df[['A','C']].mean()

# # Encuentra la estación asociada a cada registro
# print('Asignando la estación correspondiente a cada medida')
# datos_estadillo = FUNCIONES_PROCESADO.evalua_estaciones(datos_corregidos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_muestreos)

# # Encuentra las salidas al mar correspondientes 
# datos_estadillo = FUNCIONES_PROCESADO.evalua_salidas(datos_estadillo,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_salidas,tabla_muestreos)

# import time
# t_init = time.time()
 
# # Encuentra el identificador asociado a cada registro
# print('Asignando el registro correspondiente a cada medida')
# datos_estadillo = FUNCIONES_PROCESADO.evalua_registros(datos_estadillo,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)
   
# t_end = time.time()

# dt1 = t_end - t_init

# # Introduce los datos en la base de datos
# print('Introduciendo los datos en la base de datos')
# texto_insercion = FUNCIONES_PROCESADO.inserta_datos(datos_estadillo,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos,tabla_muestreos)





