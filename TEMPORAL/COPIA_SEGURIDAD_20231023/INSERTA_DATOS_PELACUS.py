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
conn_psql        = create_engine(con_engine)
df_salidas       = psql.read_sql('SELECT * FROM salidas_muestreos', conn_psql)
conn_psql.dispose()

# Carga los datos
print('Leyendo los datos contenidos en la excel')
datos_pelacus = FUNCIONES_LECTURA.lectura_datos_pelacus(archivo_datos)  
 
# Realiza un control de calidad primario a los datos importados   
print('Realizando control de calidad')
datos_pelacus_corregido = datos_pelacus[datos_pelacus['latitud'].notna()] 
datos_pelacus_corregido = datos_pelacus_corregido[datos_pelacus_corregido['longitud'].notna()] 

# # Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# Extrae el set de datos de cada año
datos_pelacus_corregido['mes'] = None
datos_pelacus_corregido['año'] = None
for idato in range(datos_pelacus_corregido.shape[0]):
    datos_pelacus_corregido['mes'].iloc[idato] = str(datos_pelacus_corregido['fecha_muestreo'].iloc[idato].month)
    datos_pelacus_corregido['año'].iloc[idato] = str(datos_pelacus_corregido['fecha_muestreo'].iloc[idato].year)

listado_años = datos_pelacus_corregido['año'].unique()

listado_años = ['2018','2019','2020','2021','2022']

for ianho in range(len(listado_años)):
#for ianho in range(1):    
    print('Procesando los datos del año ',listado_años[ianho])
    
    datos_anual = datos_pelacus_corregido[datos_pelacus_corregido['año']==listado_años[ianho]]

    datos_anual = datos_anual.replace(numpy.nan, None)

    str_salida = 'PELACUS ' + str(listado_años[ianho])
    id_salida  = df_salidas['id_salida'][df_salidas['nombre_salida']==str_salida].iloc[0]
    
    datos_anual['id_salida'] = id_salida
    
    str_salida_corto = 'PEL' + str((datos_anual['mes'].iloc[0])).zfill(2) +  str(datos_anual['año'].iloc[0])[2:4]
    
    for idato in range(datos_anual.shape[0]):
        datos_anual['estacion'].iloc[idato] = str_salida_corto + '_' + str(datos_anual['estacion'].iloc[idato])
    
    datos_anual = FUNCIONES_PROCESADO.evalua_estaciones(datos_anual,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

    # Encuentra el identificador asociado a cada registro
    print('Identificando el registro correspondiente a cada medida')
    datos_anual = FUNCIONES_PROCESADO.evalua_registros(datos_anual,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
    
    # Introduce los datos en la base de datos
    print('Introduciendo los datos en la base de datos')
    FUNCIONES_PROCESADO.inserta_datos(datos_anual,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)


 

# FUNCIONES_AUXILIARES.actualiza_estado(datos_pelacus_corregido,id_programa,programa_muestreo,fecha_actualizacion,email_contacto,itipo_informacion,direccion_host,base_datos,usuario,contrasena,puerto)


print('Procesado terminado')





