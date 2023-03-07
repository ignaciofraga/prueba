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


# Define la fecha actual
fecha_actualizacion = datetime.date.today()

### PROCESADO ###

# Listado de archivos disponibles
from os import listdir
from os.path import isfile, join
listado_archivos = [f for f in listdir(directorio_datos) if isfile(join(directorio_datos, f))]

#for iarchivo in range(len(listado_archivos)):
#for iarchivo in range(1):

#nombre_archivo = directorio_datos + '/' + listado_archivos[iarchivo]

nombre_archivo  = directorio_datos + '/RADPROF_2022.xlsm'

# Listado de archivos disponibles
from os import listdir
from os.path import isfile, join
listado_archivos = [f for f in listdir(directorio_datos) if isfile(join(directorio_datos, f))]

for iarchivo in range(len(listado_archivos)):
   
    nombre_archivo = directorio_datos + '/' + listado_archivos[iarchivo] 
   
    print('Procesando la informacion correspondiente al año ',nombre_archivo[-9:-5])
    
    print('Leyendo los datos contenidos en el archivo excel')
    datos_radprof  = FUNCIONES_LECTURA.lectura_datos_radprof(nombre_archivo)
    datos_radprof['prof_referencia'] = None
        
    # Realiza un control de calidad primario a los datos importados   
    print('Realizando control de calidad')
    datos_radprof_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_radprof,direccion_host,base_datos,usuario,contrasena,puerto)  
    
    # Recupera el identificador del programa de muestreo
    id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)
    
    # Encuentra la estación asociada a cada registro
    print('Asignando la estación correspondiente a cada medida')
    datos_radprof_corregido = FUNCIONES_PROCESADO.evalua_estaciones(datos_radprof_corregido,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)
    
    # Asigna el identificador de salida al mar correspondiente
    tipo_salida = 'ANUAL'
    datos_radprof_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos_radprof_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
    
    
    # Encuentra el identificador asociado a cada registro
    print('Asignando el registro correspondiente a cada medida')
    datos_radprof_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_radprof_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
    
    # # # # # Introduce los datos en la base de datos
    print('Introduciendo los datos en la base de datos')
    FUNCIONES_PROCESADO.inserta_datos(datos_radprof_corregido,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)
    FUNCIONES_PROCESADO.inserta_datos(datos_radprof_corregido,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)

 
    
    # # # Actualiza estado
    # # print('Actualizando el estado de los procesos')
    # FUNCIONES_AUXILIARES.actualiza_estado(datos_radprof_corregido,id_programa,programa_muestreo,fecha_actualizacion,email_contacto,itipo_informacion,direccion_host,base_datos,usuario,contrasena,puerto)
     
# print('Procesado del año ', nombre_archivo[-9:-5], ' terminado')
    
print('Fin del procesado de todos los datos disponibles')
  
    
