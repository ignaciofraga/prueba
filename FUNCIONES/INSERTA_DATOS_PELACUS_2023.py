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
import numpy
import math
import os
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
nombre_programa = 'PELACUS'
id_config_sup   = 1
id_config_per   = 1

# Rutas de los archivos a importar  
directorio_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/2023' 




# Listado de archivos disponibles
from os import listdir
from os.path import isfile, join
listado_archivos = [f for f in listdir(directorio_datos) if isfile(join(directorio_datos, f))]

#for iarchivo in range(1):
for iarchivo in range(len(listado_archivos)):

    print('Procesando archivo ',listado_archivos[iarchivo])    

    os.chdir(directorio_datos)
    nombre_archivo = listado_archivos[iarchivo]
    with open(nombre_archivo) as f:
        datos_archivo = [line.rstrip('\n') for line in f]

    mensaje_error,datos_botellas,io_par,io_fluor,io_O2 = FUNCIONES_LECTURA.lectura_btl(nombre_archivo,datos_archivo) 
    
    
    datos_botellas,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_botellas) 

    # Recupera el identificador del programa de muestreo
    id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto)
    
    # Encuentra la estación asociada a cada registro
    nombre_estacion = str(nombre_archivo[0:11])    
    datos_botellas['estacion']  = nombre_estacion 
    datos_botellas = FUNCIONES_PROCESADO.evalua_estaciones(datos_botellas,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

    
    # Asigna la salida correspondiente
    datos_botellas['id_salida'] = int(645)
    
    # Encuentra el identificador asociado a cada registro
    datos_botellas = FUNCIONES_PROCESADO.evalua_registros(datos_botellas,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
   
    FUNCIONES_PROCESADO.inserta_datos(datos_botellas,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)
   

# # Tipo de información a introducir
# itipo_informacion = 2 # 1-nuevo muestreo 2-dato nuevo (analisis laboratorio)  3-dato re-analizado (control calidad)   
# email_contacto    = 'prueba@ieo.csic.es'

# fecha_actualizacion = datetime.date.today()

# # Carga los datos
# print('Leyendo los datos contenidos en la excel')
# datos_pelacus = FUNCIONES_LECTURA.lectura_datos_pelacus(archivo_datos)  
 
# # Realiza un control de calidad primario a los datos importados   
# print('Realizando control de calidad')
# datos_pelacus_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_pelacus,direccion_host,base_datos,usuario,contrasena,puerto)  

# # Recupera el identificador del programa de muestreo
# id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra la estación asociada a cada registro
# print('Identificando la estación correspondiente a cada medida')
# datos_pelacus_corregido = FUNCIONES_PROCESADO.evalua_estaciones(datos_pelacus_corregido,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# # Asigna el identificador de salida al mar correspondiente
# tipo_salida = 'ANUAL'
# datos_pelacus_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos_pelacus_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra el identificador asociado a cada registro
# print('Identificando el registro correspondiente a cada medida')
# datos_pelacus_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_pelacus_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# # Introduce los datos en la base de datos
# print('Introduciendo los datos en la base de datos')
# FUNCIONES_PROCESADO.inserta_datos(datos_pelacus_corregido,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)
# FUNCIONES_PROCESADO.inserta_datos(datos_pelacus_corregido,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)

 

# # FUNCIONES_AUXILIARES.actualiza_estado(datos_pelacus_corregido,id_programa,programa_muestreo,fecha_actualizacion,email_contacto,itipo_informacion,direccion_host,base_datos,usuario,contrasena,puerto)


# print('Procesado terminado')





