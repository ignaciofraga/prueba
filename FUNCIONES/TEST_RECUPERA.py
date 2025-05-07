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
from sqlalchemy import create_engine, text


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'ignacio'
contrasena     = '1gn4c10'
puerto         = '5432'
direccion_host = '193.146.155.72'
#193.146.155.72


direccion_host = "172.20.0.202"
port           = 5432
base_datos     = "COAC"
usuario        = "ignacio"
contrasena     = "1gn4c10"

# Parámetros
anho_consulta = 2023
programa_seleccionado = 'RADIAL VIGO'
tipo_salida           = 'SEMANAL'
###### PROCESADO ########



# Recupera datos de la base de datos

con_engine         = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn               = create_engine(con_engine)
df_programas       = pandas.DataFrame(conn.connect().execute(text('SELECT * FROM programas')))
variables_bd       = pandas.DataFrame(conn.connect().execute(text('SELECT * FROM variables_procesado')))
df_salidas         = pandas.DataFrame(conn.connect().execute(text('SELECT * FROM salidas_muestreos')))
df_muestreos       = pandas.DataFrame(conn.connect().execute(text('SELECT * FROM muestreos_discretos')))
df_datos_discretos = pandas.DataFrame(conn.connect().execute(text('SELECT * FROM datos_discretos')))
conn.dispose() 



# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)


df_salidas_programa = df_salidas[(df_salidas['programa']==id_programa) & (df_salidas['tipo_salida']==tipo_salida)]
#df_salidas_programa = df_salidas[df_salidas['programa']==id_programa]

df_salidas_programa['año'] = None
for idato in range(df_salidas_programa.shape[0]):
    df_salidas_programa['año'].iloc[idato] = (df_salidas_programa['fecha_salida'].iloc[idato]).year

df_salidas_seleccion = df_salidas_programa[df_salidas_programa['año']==anho_consulta]
listado_salidas      = df_salidas_seleccion['id_salida']

#listado_salidas = [81]


df_muestreos_seleccionados = df_muestreos[df_muestreos['salida_mar'].isin(listado_salidas)]

df_muestreos_seleccionados = df_muestreos_seleccionados.sort_values(by=['muestreo'])

df_datos = pandas.merge(df_muestreos_seleccionados, df_datos_discretos, on="muestreo")

#df_datos = df_datos[df_datos['carbono_organico_total'].notna()]