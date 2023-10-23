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
programa_seleccionado = 'RADPROF'
tipo_salida           = 'ANUAL'
id_config_sup     = 1
id_config_per     = 1

# Rutas de los archivos a importar  
#archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx' 
archivo_datos                ='C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADPROF/RADPROF_2023.xlsx'

archivo_datos                ='C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADPROF2023/BTL_RADPROF23_RED.xlsx'

# Tipo de información a introducir
itipo_informacion = 2 # 1-nuevo muestreo 2-dato nuevo (analisis laboratorio)  3-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'



###### PROCESADO ########

con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn             = create_engine(con_engine)
df_programas     = psql.read_sql('SELECT * FROM programas', conn)
tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
tabla_variables  = psql.read_sql('SELECT * FROM variables_procesado', conn)
tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
conn.dispose() 


df_datos_importacion  = pandas.read_excel(archivo_datos) 

# control de calidad
datos_corregidos,textos_aviso   = FUNCIONES_PROCESADO.control_calidad(df_datos_importacion)

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)



# Encuentra la estación asociada a cada registro
datos_corregidos = FUNCIONES_PROCESADO.evalua_estaciones(datos_corregidos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_muestreos)

# Encuentra las salidas al mar correspondientes  
nombre_entrada = programa_seleccionado 
datos_corregidos = FUNCIONES_PROCESADO.evalua_salidas(datos_corregidos,id_programa,nombre_entrada,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_salidas,tabla_muestreos)
 
# Encuentra el identificador asociado a cada registro

datos_corregidos = FUNCIONES_PROCESADO.evalua_registros(datos_corregidos,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)


# # Añade datos físicos      
# FUNCIONES_PROCESADO.inserta_datos(datos_corregidos,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables)
    


