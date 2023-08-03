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
#import seawater
#import FUNCIONES_LECTURA
#import FUNCIONES_PROCESADO
import pandas
pandas.options.mode.chained_assignment = None
import datetime
#import seawater

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

#fecha_umbral = datetime.date(2018,1,1)

# Recupera la tabla con los registros de los muestreos
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn        = create_engine(con_engine)
df_muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
df_estaciones             = psql.read_sql('SELECT * FROM estaciones', conn)
df_datos_discretos        = psql.read_sql('SELECT * FROM datos_discretos', conn)
df_salidas                = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
df_programas              = psql.read_sql('SELECT * FROM programas', conn)
df_indices_calidad        = psql.read_sql('SELECT * FROM indices_calidad', conn)
conn.dispose()   


# Combina la información de muestreos y salidas en un único dataframe 
df_muestreos          = df_muestreos.rename(columns={"salida_mar": "id_salida"}) # Para igualar los nombres de columnas                                               
df_muestreos          = pandas.merge(df_muestreos, df_salidas, on="id_salida")
df_muestreos          = df_muestreos.rename(columns={"id_salida": "salida_mar"}) # Deshaz el cambio de nombre
                 


# compón un dataframe con la información de muestreo y datos biogeoquímicos                                            
df_datos_disponibles  = pandas.merge(df_datos_discretos, df_muestreos, on="muestreo")

# Añade columna con información del año
df_datos_disponibles['año'] = pandas.DatetimeIndex(df_datos_disponibles['fecha_muestreo']).year


# procesa ese dataframe
io_control_calidad = 1
#indice_programa,indice_estacion,indice_salida,cast_seleccionado,meses_offset,variable_seleccionada,salida_seleccionada = FUNCIONES_AUXILIARES.menu_seleccion(df_datos_disponibles,variables_procesado,variables_procesado_bd,io_control_calidad,df_salidas,df_estaciones,df_programas)
indice_programa       = 3
indice_estacion       = 1
indice_salida         = 180
cast_seleccionado     = 1
meses_offset          = 1
#variable_seleccionada = 'temperatura_ctd'
variable_seleccionada = 'toc'                                      

df_datos_disponibles_store = df_datos_disponibles
datos_procesados     = df_datos_disponibles[df_datos_disponibles["salida_mar"] == indice_salida]

datos_variable = datos_procesados[['estacion','botella','presion_ctd',variable_seleccionada]]
datos_variable = datos_variable.sort_values(['estacion', 'presion_ctd'], ascending=[True, False])

