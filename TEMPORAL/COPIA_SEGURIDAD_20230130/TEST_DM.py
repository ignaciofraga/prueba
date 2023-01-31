# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 08:15:20 2022

@author: ifraga
"""

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

import numpy
import pandas
import datetime
import numpy
import pandas
import datetime
from pyproj import Proj
import math
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine
import json
import re

import FUNCIONES_PROCESADO
import FUNCIONES_LECTURA


programa_seleccionado = 'RADIAL CORUÑA'
tipo_salida           = 'MENSUAL'

archivo_datos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/PRUEBA_WEB/prueba/EJEMPLOS/EXCEL_ENTRADA.xlsx'

con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
df_programas              = psql.read_sql('SELECT * FROM programas', conn_psql)
variables_bd              = psql.read_sql('SELECT * FROM variables_procesado', conn_psql)
conn_psql.dispose()





# Lectura del archivo con los resultados del AA
df_datos_importacion =pandas.read_excel(archivo_datos)

variables_archivo = df_datos_importacion.columns.tolist()
variables_fisica  = list(set(variables_bd['variables_fisicas']).intersection(variables_archivo))
variables_bgq     = list(set(variables_bd['variables_biogeoquimicas']).intersection(variables_archivo))




# corrige el formato de las fechas
for idato in range(df_datos_importacion.shape[0]):
    df_datos_importacion['fecha_muestreo'][idato] = (df_datos_importacion['fecha_muestreo'][idato]).date()
    if df_datos_importacion['fecha_muestreo'][idato]:
        df_datos_importacion['hora_muestreo'][idato] = datetime.datetime.strptime(df_datos_importacion['hora_muestreo'][idato], '%H:%M:%S').time()

# Realiza un control de calidad primario a los datos importados   
datos_corregidos,textos_aviso   = FUNCIONES_PROCESADO.control_calidad(df_datos_importacion,direccion_host,base_datos,usuario,contrasena,puerto)  

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)


#with st.spinner('Asignando la estación y salida al mar de cada medida'):
# Encuentra la estación asociada a cada registro
datos_corregidos = FUNCIONES_PROCESADO.evalua_estaciones(datos_corregidos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# Encuentra las salidas al mar correspondientes  
datos_corregidos = FUNCIONES_PROCESADO.evalua_salidas(datos_corregidos,id_programa,programa_seleccionado,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
 
# Encuentra el identificador asociado a cada registro
#with st.spinner('Asignando el registro correspondiente a cada medida'):
datos_corregidos = FUNCIONES_PROCESADO.evalua_registros(datos_corregidos,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
   

if len(variables_fisica)>0:
    
    listado_aux   = ['id_muestreo_temp'] + variables_fisica 
    datos_fisica  = datos_corregidos[listado_aux] 
    
    str_variables = ','.join(variables_fisica)
    str_valores   = ',%s'*len(variables_fisica)
    listado_excluded = ['EXCLUDED.' + var for var in variables_fisica]
    str_exclude   = ','.join(listado_excluded)
    for idato in range(datos_corregidos.shape[0]):
                            
        instruccion_sql = "INSERT INTO datos_discretos_fisica (muestreo," + str_variables + ") VALUES (%s" +  str_valores + ") ON CONFLICT (muestreo) DO UPDATE SET (" + str_variables + ") = ROW(" + str_exclude + ");"                            
        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
        cursor = conn.cursor()
        cursor.execute(instruccion_sql, (datos_fisica.iloc[idato]))
        conn.commit()



# Introduce los datos en la base de datos
#with st.spinner('Intoduciendo la información en la base de datos'):

# FUNCIONES_PROCESADO.inserta_datos_fisica(datos_corregidos,direccion_host,base_datos,usuario,contrasena,puerto)

# FUNCIONES_PROCESADO.inserta_datos_biogeoquimica(datos_corregidos,direccion_host,base_datos,usuario,contrasena,puerto)


    
    

