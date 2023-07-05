# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 08:27:43 2023

@author: ifraga
"""

import pandas
import datetime
import pandas.io.sql as psql
from sqlalchemy import create_engine
import FUNCIONES_PROCESADO

pandas.options.mode.chained_assignment = None


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

fecha_umbral = datetime.date(2018,1,1)

# Recupera la tabla con los registros de los muestreos
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn        = create_engine(con_engine)
df_muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
df_estaciones             = psql.read_sql('SELECT * FROM estaciones', conn)
df_datos_biogeoquimicos   = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn)
df_datos_fisicos          = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn)
df_datos                  = psql.read_sql('SELECT * FROM datos_discretos', conn)
df_salidas                = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
df_programas              = psql.read_sql('SELECT * FROM programas', conn)
df_indices_calidad        = psql.read_sql('SELECT * FROM indices_calidad', conn)
conn.dispose()   

FUNCIONES_PROCESADO.inserta_datos(df_datos_biogeoquimicos,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)



#df_datos_disponibles  = pandas.merge(df_muestreos, df_datos_fisicos, on="muestreo")

#df_datos_disponibles  = pandas.merge(df_datos_fisicos, df_datos_biogeoquimicos, on="muestreo")

#FUNCIONES_PROCESADO.inserta_datos(df_datos_disponibles,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)



#df_datos_disponibles  = pandas.merge(df_datos_biogeoquimicos, df_muestreos, on="muestreo")

#FUNCIONES_PROCESADO.inserta_datos(df_datos_disponibles,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)




