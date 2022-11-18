# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""

import FUNCIONES_INSERCION
import pandas
pandas.options.mode.chained_assignment = None
import datetime

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
archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx' 

# Tipo de información a introducir
itipo_informacion = 1 # 1-dato nuevo (analisis laboratorio)  2-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'

fecha_actualizacion = datetime.date.today()

###### PROCESADO ########

# Carga los datos
print('Leyendo los datos contenidos en la excel')
datos_pelacus = FUNCIONES_INSERCION.lectura_datos_pelacus(archivo_datos)  
 
# Realiza un control de calidad primario a los datos importados   
print('Realizando control de calidad')
datos_pelacus_corregido,textos_aviso = FUNCIONES_INSERCION.control_calidad(datos_pelacus,direccion_host,base_datos,usuario,contrasena,puerto)  

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_INSERCION.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# Encuentra la estación asociada a cada registro
print('Identificando la estación correspondiente a cada medida')
datos_pelacus_corregido = FUNCIONES_INSERCION.evalua_estaciones(datos_pelacus_corregido,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# Asigna el identificador de salida al mar correspondiente
tipo_salida = 'ANUAL'
datos_pelacus_corregido = FUNCIONES_INSERCION.evalua_salidas(datos_pelacus_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)

# Encuentra el identificador asociado a cada registro
print('Identificando el registro correspondiente a cada medida')
datos_pelacus_corregido = FUNCIONES_INSERCION.evalua_registros(datos_pelacus_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# Introduce los datos en la base de datos
print('Introduciendo los datos en la base de datos')
FUNCIONES_INSERCION.inserta_datos_fisica(datos_pelacus_corregido,direccion_host,base_datos,usuario,contrasena,puerto)
FUNCIONES_INSERCION.inserta_datos_biogeoquimica(datos_pelacus_corregido,direccion_host,base_datos,usuario,contrasena,puerto)

print('Procesado terminado')





