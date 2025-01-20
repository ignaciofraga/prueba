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
import numpy

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'






# 1	"PELACUS"
# 2	"RADIAL VIGO"
# 3	"RADIAL CORUÑA"
# 4	"RADIAL CANTABRICO"
# 5	"RADPROF"
# 6	"OTROS"


id_programa = 4

con_engine          = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql           = create_engine(con_engine)

# Recupera las estaciones del programa seleccionado
instruccion_SQL        = " SELECT * FROM estaciones WHERE programa = %(idprograma)s"  
df_estaciones_programa = pandas.read_sql_query(instruccion_SQL, con=conn_psql, params={'idprograma':int(id_programa)})

# Recupera las estaciones del programa seleccionado
instruccion_SQL        = " SELECT * FROM programas WHERE id_programa = %(idprograma)s"  
df_programas           = pandas.read_sql_query(instruccion_SQL, con=conn_psql, params={'idprograma':int(id_programa)})
abreviatura_programa   = df_programas['abreviatura'].iloc[0]

# Recupera las salidas del programa seleccionado
instruccion_SQL     = " SELECT * FROM salidas_muestreos WHERE programa = %(idprograma)s"  
df_salidas_programa = pandas.read_sql_query(instruccion_SQL, con=conn_psql, params={'idprograma':int(id_programa)})

#Itera en los muestreos de cada salida
instruccion_actualiza = 'UPDATE muestreos_discretos SET nombre_muestreo =%s WHERE muestreo = %s;'
instruccion_SQL       = " SELECT * FROM muestreos_discretos WHERE salida_mar = %(idsalida)s"  
for isalida in range(df_salidas_programa.shape[0]):
    df_muestreos_salida = pandas.read_sql_query(instruccion_SQL, con=conn_psql, params={'idsalida':int(df_salidas_programa['id_salida'].iloc[isalida])})    


    # conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    # cursor = conn.cursor()

    for idato in range(df_muestreos_salida.shape[0]):
        nombre_anterior = df_muestreos_salida['nombre_muestreo'].iloc[idato]
        
        nombre_estacion                              = df_estaciones_programa.loc[df_estaciones_programa['id_estacion'] == df_muestreos_salida['estacion'].iloc[idato]]['nombre_estacion'].iloc[0]
        
        nombre_muestreo     = abreviatura_programa + '_' + df_muestreos_salida['fecha_muestreo'].iloc[idato].strftime("%Y%m%d") + '_' + str(nombre_estacion)
        if df_muestreos_salida['num_cast'].iloc[idato] is not None:
            nombre_muestreo = nombre_muestreo + '_C' + str(round(df_muestreos_salida['num_cast'].iloc[idato]))
        else:
            nombre_muestreo = nombre_muestreo + '_C1' 
            
        if df_muestreos_salida['botella'].iloc[idato] is not None:
            nombre_muestreo = nombre_muestreo + '_B' + str(round(df_muestreos_salida['botella'].iloc[idato])) 
        else:
            if df_muestreos_salida['prof_teorica'].iloc[idato] is not None: 
                nombre_muestreo = nombre_muestreo + '_P' + str(round(df_muestreos_salida['prof_teorica'].iloc[idato]))
            else:
                nombre_muestreo = nombre_muestreo + '_P' + str(round(df_muestreos_salida['presion_ctd'].iloc[idato])) 
         
        print('O ',nombre_anterior)
        print('N ',nombre_muestreo)

    #     cursor.execute(instruccion_actualiza, (nombre_muestreo,int(df_muestreos_salida['muestreo'].iloc[idato])))
    #     conn.commit()

    # cursor.close()
    # conn.close()

