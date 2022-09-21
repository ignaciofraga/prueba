# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""







import FUNCIONES_INSERCION
import pandas
pandas.options.mode.chained_assignment = None
import datetime
import numpy

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_muestreo = 'RADPROF'

directorio_datos             = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADPROF'

itipo_informacion = 1 # 1-dato nuevo (analisis laboratorio)  2-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'


fecha_actualizacion = datetime.date.today()

### PROCESADO ###

# Recupera el identificador del programa de muestreo
id_programa = FUNCIONES_INSERCION.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

archivo_datos = directorio_datos + '/RADPROF_2021.xlsm'

# Lee la informacion 
datos = FUNCIONES_INSERCION.lectura_datos_radprof(archivo_datos)


# Realiza un control de calidad primario a los datos importados   
print('Realizando control de calidad')
datos,textos_aviso = FUNCIONES_INSERCION.control_calidad(datos,direccion_host,base_datos,usuario,contrasena,puerto) 
 

# Introduce los datos en la base de datos
print('Introduciendo los datos en la base de datos')

datos = FUNCIONES_INSERCION.evalua_estaciones(datos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)  

datos = FUNCIONES_INSERCION.evalua_registros(datos,programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

FUNCIONES_INSERCION.inserta_datos_fisica(datos,direccion_host,base_datos,usuario,contrasena,puerto)

FUNCIONES_INSERCION.inserta_datos_biogeoquimica(datos,direccion_host,base_datos,usuario,contrasena,puerto)
         
# Actualiza estado
print('Actualizando el estado de los procesos')
FUNCIONES_INSERCION.actualiza_estado(datos,fecha_actualizacion,id_programa,programa_muestreo,itipo_informacion,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto)


    
# print('Fin del procesado de todos los datos disponibles')
  
    
