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

# #########################################################
# ## TABLA CON LOS ESTADOS DEL PROCESADO DE LAS CAMPAÑAS ##
# #########################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'estado_procesos'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_proceso SERIAL PRIMARY KEY,'
# ' programa int NOT NULL,'
# ' nombre_programa text NOT NULL,'
# ' año int NOT NULL,'
# ' fecha_analisis_laboratorio date,'
# ' analisis_finalizado bool NOT NULL,'
# ' campaña_realizada bool NOT NULL,'
# ) 

# listado_dependencias = ('FOREIGN KEY (programa)'
#   'REFERENCES programas (id_programa)'
#   ' ON UPDATE CASCADE '
#   'ON DELETE CASCADE')

# listado_unicidades = (', UNIQUE (programa,año))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()





# 1	"PELACUS"
# 2	"RADIAL VIGO"
# 3	"RADIAL CORUÑA"
# 4	"RADIAL CANTABRICO"
# 5	"RADPROF"
# 6	"OTROS"


id_programa = 2
nombre_programa = 'RADIAL VIGO'

con_engine          = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql           = create_engine(con_engine)

# Recupera las salidas del programa seleccionado
instruccion_SQL     = " SELECT * FROM salidas_muestreos WHERE programa = %(idprograma)s"  
df_salidas_programa = pandas.read_sql_query(instruccion_SQL, con=conn_psql, params={'idprograma':int(id_programa)})

# Añade el año de muestreo al dataframe
df_salidas_programa['año'] = None
for isalida in range(df_salidas_programa.shape[0]):
    df_salidas_programa['año'].iloc[isalida] = df_salidas_programa['fecha_salida'].iloc[isalida].year 

df_salidas_programa = df_salidas_programa.sort_values('fecha_salida')
anho_inicio         = min(df_salidas_programa['año'])
anho_final          = datetime.date.today().year


conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

fecha_actualizacion = datetime.date.today()


instruccion_actualiza = 'INSERT INTO estado_procesos (programa,nombre_programa,año,fecha_analisis_laboratorio,analisis_finalizado,campaña_realizada) VALUES (%s,%s,%s,%s,%s,%s) ;' 

for ianho in range(anho_inicio,anho_final+1):
    print(ianho)
    cursor.execute(instruccion_actualiza, (id_programa,nombre_programa,ianho,fecha_actualizacion,True,True))
    conn.commit()
    
cursor.close()
conn.close()

# #Itera en los muestreos de cada salida
# instruccion_actualiza = 'UPDATE muestreos_discretos SET nombre_muestreo =%s WHERE muestreo = %s;'
# instruccion_SQL       = " SELECT * FROM muestreos_discretos WHERE salida_mar = %(idsalida)s"  
# for isalida in range(df_salidas_programa.shape[0]):
#     df_muestreos_salida = pandas.read_sql_query(instruccion_SQL, con=conn_psql, params={'idsalida':int(df_salidas_programa['id_salida'].iloc[isalida])})    


#     # conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
#     # cursor = conn.cursor()

#     for idato in range(df_muestreos_salida.shape[0]):
#         nombre_anterior = df_muestreos_salida['nombre_muestreo'].iloc[idato]
        
#         nombre_estacion                              = df_estaciones_programa.loc[df_estaciones_programa['id_estacion'] == df_muestreos_salida['estacion'].iloc[idato]]['nombre_estacion'].iloc[0]
        
#         nombre_muestreo     = abreviatura_programa + '_' + df_muestreos_salida['fecha_muestreo'].iloc[idato].strftime("%Y%m%d") + '_' + str(nombre_estacion)
#         if df_muestreos_salida['num_cast'].iloc[idato] is not None:
#             nombre_muestreo = nombre_muestreo + '_C' + str(round(df_muestreos_salida['num_cast'].iloc[idato]))
#         else:
#             nombre_muestreo = nombre_muestreo + '_C1' 
            
#         if df_muestreos_salida['botella'].iloc[idato] is not None:
#             nombre_muestreo = nombre_muestreo + '_B' + str(round(df_muestreos_salida['botella'].iloc[idato])) 
#         else:
#             if df_muestreos_salida['prof_teorica'].iloc[idato] is not None: 
#                 nombre_muestreo = nombre_muestreo + '_P' + str(round(df_muestreos_salida['prof_teorica'].iloc[idato]))
#             else:
#                 nombre_muestreo = nombre_muestreo + '_P' + str(round(df_muestreos_salida['presion_ctd'].iloc[idato])) 
         
#         print('O ',nombre_anterior)
#         print('N ',nombre_muestreo)

    #     cursor.execute(instruccion_actualiza, (nombre_muestreo,int(df_muestreos_salida['muestreo'].iloc[idato])))
    #     conn.commit()

    # cursor.close()
    # conn.close()

