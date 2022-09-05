# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""

import FUNCIONES_INSERCION
import pandas
pandas.options.mode.chained_assignment = None

# Parámetros de la base de datos
base_datos = 'IEO_Coruna'
usuario    = 'postgres'
contrasena = 'IEO2022'
puerto     = '5432'

# Parámetros
min_dist          = 50 # minima distancia para considerar dos estaciones diferentes
programa_muestreo = 'PELACUS'
id_config_sup     = 1
id_config_per     = 1

# Rutas de los archivos a importar  
archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx' 
archivo_variables_base_datos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/VARIABLES.xlsx'  

# Tipo de información a introducir
itipo_informacion = 1 # 1-dato nuevo (analisis laboratorio)  2-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'

###### PROCESADO ########

# Carga los datos
print('Leyendo los datos contenidos en la excel')
datos_pelacus = FUNCIONES_INSERCION.lectura_datos_pelacus(archivo_datos,direccion_host,base_datos,usuario,contrasena,puerto)  
 
# Realiza un control de calidad primario a los datos importados   
print('Realizando control de calidad')
datos_pelacus_corregido = FUNCIONES_INSERCION.control_calidad(datos_pelacus,archivo_variables_base_datos)  
 
# Recupera el identificador del programa de muestreo
id_programa = FUNCIONES_INSERCION.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# Introduce los datos en la base de datos
print('Introduciendo los datos en la base de datos')
FUNCIONES_INSERCION.inserta_datos(datos_pelacus_corregido,min_dist,programa_muestreo,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)
      
# Actualiza estado
print('Actualizando el estado de los procesos')
FUNCIONES_INSERCION.actualiza_estado(datos_pelacus_corregido,id_programa,programa_muestreo,itipo_informacion,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto)

print('Procesado terminado')





