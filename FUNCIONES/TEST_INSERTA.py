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
programa_muestreo = 'RADIAL CANTABRICO'
tipo_salida       = 'MENSUAL'


con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn             = create_engine(con_engine)
tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
tabla_datos      = psql.read_sql('SELECT * FROM datos_discretos', conn)
tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
tabla_variables  = psql.read_sql('SELECT * FROM variables_procesado', conn)
conn.dispose() 


# Rutas de los archivos a importar  
#archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx' 
#archivo_datos                ='C:/Users/ifraga/Downloads/PROCESADO_230913_RCAN21_SepDic_profR1R1.xlsx'
#archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIAL CANTABRICO/2020/RADCAN_2020.xlsx'
# Importa el .xlsx
#df_datos_importacion = pandas.read_excel(archivo_datos,index_col=None)

           



archivo_datos                ='C:/Users/ifraga/Downloads/2024-07-03T09-24_export.csv'
df_datos_importacion = pandas.read_csv(archivo_datos,index_col=None)
datos_estadillo = df_datos_importacion

import numpy
datos_estadillo = datos_estadillo.dropna(subset = ['muestreo'])
# Define una columna índice
indices_dataframe         = numpy.arange(0,datos_estadillo.shape[0],1,dtype=int)
datos_estadillo['id_temp'] = indices_dataframe
datos_estadillo.set_index('id_temp',drop=True,append=False,inplace=True)


# Realiza un control de calidad primario a los datos importados   
#datos_corregidos,textos_aviso   = FUNCIONES_PROCESADO.control_calidad(df_datos_importacion)  

# Convierte las fechas de DATE a formato correcto
#datos_corregidos['fecha_muestreo'] =  pandas.to_datetime(datos_corregidos['fecha_muestreo'], format='%d/%m/%Y').dt.date


## # Recupera el identificador del programa de muestreo
#id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)
        
# Encuentra la estación asociada a cada registro
#print('Asignando la estación correspondiente a cada medida')
#datos_estadillo = FUNCIONES_PROCESADO.evalua_estaciones(datos_corregidos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_muestreos)

# # Encuentra las salidas al mar correspondientes 
#datos_estadillo = FUNCIONES_PROCESADO.evalua_salidas(datos_estadillo,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_salidas,tabla_muestreos)

# # Encuentra el identificador asociado a cada registro
# print('Asignando el registro correspondiente a cada medida')
#datos_estadillo = FUNCIONES_PROCESADO.evalua_registros(datos_estadillo,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)


#datos_insercion = datos_estadillo.dropna(subset = ['muestreo'])


# # Introduce los datos en la base de datos
# print('Introduciendo los datos en la base de datos')
texto_insercion = FUNCIONES_PROCESADO.inserta_datos(datos_estadillo,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos,tabla_muestreos)





