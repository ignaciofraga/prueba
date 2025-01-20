# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""

import FUNCIONES_LECTURA
import FUNCIONES_PROCESADO
import numpy
import pandas
pandas.options.mode.chained_assignment = None
import pandas.io.sql as psql
from sqlalchemy import create_engine
import psycopg2
import os

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_muestreo = 'PELACUS'
id_config_sup     = 1
id_config_per     = 1

# Rutas de los archivos a importar  
archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2022.xlsx' 

# Tipo de información a introducir
itipo_informacion = 2 # 1-nuevo muestreo 2-dato nuevo (analisis laboratorio)  3-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'


con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn             = create_engine(con_engine)
df_programas     = psql.read_sql('SELECT * FROM programas', conn)
tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
tabla_variables  = psql.read_sql('SELECT * FROM variables_procesado', conn)
tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
conn.dispose() 

# # Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra las salidas del programa
# listado_salidas = tabla_salidas['id_salida'][tabla_salidas['programa']==id_programa]

# for isalida in range(len(listado_salidas)):

#     # Inserta en la base de datos
#     conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
#     cursor = conn.cursor()                      
#     instruccion_sql = 'DELETE FROM muestreos_discretos WHERE salida_mar = ' + str(listado_salidas.iloc[isalida]) + ';'        
#     cursor.execute(instruccion_sql)
#     conn.commit()
#     cursor.close()
#     conn.close()




# # Carga los datos
# print('Leyendo los datos contenidos en la excel')
# datos_pelacus = FUNCIONES_LECTURA.lectura_datos_pelacus(archivo_datos)  
 
# # Realiza un control de calidad primario a los datos importados   
# print('Realizando control de calidad')
# datos_pelacus_latlon = datos_pelacus[datos_pelacus['latitud'].notna()] 
# datos_pelacus_latlon = datos_pelacus_latlon[datos_pelacus_latlon['longitud'].notna()]
# datos_pelacus_corregido,texto = FUNCIONES_PROCESADO.control_calidad(datos_pelacus_latlon)


# # Extrae el set de datos de cada año
# datos_pelacus_corregido['mes'] = None
# datos_pelacus_corregido['año'] = None
# for idato in range(datos_pelacus_corregido.shape[0]):
#     datos_pelacus_corregido['mes'].iloc[idato] = str(datos_pelacus_corregido['fecha_muestreo'].iloc[idato].month)
#     datos_pelacus_corregido['año'].iloc[idato] = str(datos_pelacus_corregido['fecha_muestreo'].iloc[idato].year)

# listado_años = datos_pelacus_corregido['año'].unique()

# for ianho in range(len(listado_años)):
   
#     print('Procesando los datos del año ',listado_años[ianho])
    
#     con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
#     conn             = create_engine(con_engine)
#     tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
#     tabla_datos      = psql.read_sql('SELECT * FROM datos_discretos', conn)
#     conn.dispose() 

    
#     datos_anual = datos_pelacus_corregido[datos_pelacus_corregido['año']==listado_años[ianho]]
#     datos_anual = datos_anual.dropna(axis=1, how='all')

#     #datos_anual = datos_anual.replace(numpy.nan, None)
    
#     str_salida = 'PELACUS ' + str(listado_años[ianho])
#     id_salida  = tabla_salidas['id_salida'][tabla_salidas['nombre_salida']==str_salida].iloc[0]
    
#     datos_anual['id_salida'] = id_salida
    
#     str_salida_corto = 'PEL' + str((datos_anual['mes'].iloc[0])).zfill(2) +  str(datos_anual['año'].iloc[0])[2:4]
    
#     for idato in range(datos_anual.shape[0]):
#         datos_anual['estacion'].iloc[idato] = str_salida_corto + '_' + str(datos_anual['estacion'].iloc[idato])
    
#     datos_anual = FUNCIONES_PROCESADO.evalua_estaciones(datos_anual,id_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_muestreos)

#     # Encuentra el identificador asociado a cada registro
#     print('Identificando el registro correspondiente a cada medida')
#     datos_anual = FUNCIONES_PROCESADO.evalua_registros(datos_anual,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)
        
#     # Introduce los datos en la base de datos
#     print('Introduciendo los datos en la base de datos')
#     texto_insercion = FUNCIONES_PROCESADO.inserta_datos(datos_anual,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos,tabla_muestreos)
 
    
 
    
 
    
 

# AÑADE DATOS DE LA SALIDA DEL 2023 (conjunto de archivo de botelleros)

directorio_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/2023' 

# Listado de archivos disponibles
from os import listdir
from os.path import isfile, join
listado_archivos = [f for f in listdir(directorio_datos) if isfile(join(directorio_datos, f))]

df_datos_salida = pandas.DataFrame()

con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn             = create_engine(con_engine)
tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
tabla_datos      = psql.read_sql('SELECT * FROM datos_discretos', conn)
conn.dispose() 


#for iarchivo in range(1):
for iarchivo in range(len(listado_archivos)):

    print('Procesando archivo ',listado_archivos[iarchivo])    

    os.chdir(directorio_datos)
    nombre_archivo = listado_archivos[iarchivo]
    with open(nombre_archivo) as f:
        datos_archivo = [line.rstrip('\n') for line in f]

    mensaje_error,datos_botellas,io_par,io_fluor,io_O2 = FUNCIONES_LECTURA.lectura_btl(nombre_archivo,datos_archivo) 
       
    datos_botellas,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_botellas) 
    
    # Encuentra la estación asociada a cada registro
    nombre_estacion = str(nombre_archivo[0:11])    
    datos_botellas['estacion']  = nombre_estacion 
    datos_botellas = FUNCIONES_PROCESADO.evalua_estaciones(datos_botellas,id_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_muestreos)

    
    # Asigna la salida correspondiente
    str_salida = 'PELACUS 2023'
    id_salida  = tabla_salidas['id_salida'][tabla_salidas['nombre_salida']==str_salida].iloc[0]
    datos_botellas['id_salida'] = int(id_salida)
    
    df_datos_salida = pandas.concat([df_datos_salida,datos_botellas])
    
# Encuentra el identificador asociado a cada registro
datos_botellas = FUNCIONES_PROCESADO.evalua_registros(df_datos_salida,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)

# Inserta los datos en la base de datos
texto_insercion = FUNCIONES_PROCESADO.inserta_datos(datos_botellas,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos,tabla_muestreos)







