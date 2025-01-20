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

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
anho_consulta = 2023
programa_seleccionado = 'RADIAL VIGO'
tipo_salida           = 'SEMANAL'
###### PROCESADO ########


con_engine         = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn               = create_engine(con_engine)
df_programas       = psql.read_sql('SELECT * FROM programas', conn)
variables_bd       = psql.read_sql('SELECT * FROM variables_procesado', conn)
df_salidas         = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
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

# Introducir los valores en la base de datos
conn   = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()  

for isalida in range(len(listado_salidas)):

    instruccion_sql = "DELETE FROM muestreos_discretos WHERE salida_mar  = " + str(int(listado_salidas.iloc[isalida])) + ";"     
    cursor.execute(instruccion_sql)
    conn.commit() 

cursor.close()
conn.close()   

