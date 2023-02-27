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
abreviatura_programa  = 'RADCOR'


archivo_datos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/PRUEBA_WEB/prueba/EJEMPLOS/EXCEL_ENTRADA.xlsx'

con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
df_programas              = psql.read_sql('SELECT * FROM programas', conn_psql)
variables_bd              = psql.read_sql('SELECT * FROM variables_procesado', conn_psql)
conn_psql.dispose()



# Recupera el identificador de la salida seleccionada
id_salida                   = 293

fecha_salida                = datetime.date(2023,1,24)



  
    
# Conecta con la base de datos
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor() 


    
archivo_subido = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/MENSUALES/Procesados/2023/rad_men_2023_01_24_0485/btl+PAR+flscufa+O2/20230124e2.btl'
nombre_archivo = '20230124e2.btl'


lectura_archivo = open(archivo_subido, "r")  
datos_archivo = lectura_archivo.readlines()

# # Lee los datos de cada archivo de botella
# nombre_archivo = archivo_subido.name
# datos_archivo = archivo_subido.getvalue().decode('utf-8').splitlines()            

# Comprueba que la fecha del archivo y de la salida coinciden
fecha_salida_texto    = nombre_archivo[0:8]
fecha_salida_archivo  = datetime.datetime.strptime(fecha_salida_texto, '%Y%m%d').date()

if fecha_salida_archivo == fecha_salida:

    # Lee datos de botellas
    mensaje_error,datos_botellas,io_par,io_fluor,io_O2 = FUNCIONES_LECTURA.lectura_btl(nombre_archivo,datos_archivo,programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)
   

cursor.close()
conn.close()   

    

