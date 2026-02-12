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
programa_muestreo = 'PELACUS'
tipo_salida       = 'ANUAL'


# con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn             = create_engine(con_engine)
# tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
# tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
# tabla_datos      = psql.read_sql('SELECT * FROM datos_discretos', conn)
# tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
# tabla_variables  = psql.read_sql('SELECT * FROM variables_procesado', conn)
# conn.dispose() 


# Rutas de los archivos a importar  
# Rutas de los archivos a importar  
archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2022.xlsx' 

print('Leyendo los datos contenidos en la excel')
datos_pelacus = FUNCIONES_LECTURA.lectura_datos_pelacus(archivo_datos)

df = datos_pelacus.drop_duplicates(subset=['estacion'])  

df['año']=None
for idato in range(df.shape[0]):
   df['año'].iloc[idato] = df['fecha_muestreo'].iloc[idato].year 

