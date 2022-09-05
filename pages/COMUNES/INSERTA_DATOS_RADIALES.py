# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""







import FUNCIONES_INSERCION
import pandas
pandas.options.mode.chained_assignment = None


# Parámetros de la base de datos
base_datos     = 'IEO_Coruna'
usuario        = 'postgres'
contrasena     = 'IEO2022'
puerto         = '5432'
direccion_host = 'localhost'


base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
min_dist          = 50 # minima distancia para considerar dos estaciones diferentes
programa_muestreo = 'RADIAL CORUÑA'

archivo_variables_base_datos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/VARIABLES.xlsx'  
directorio_datos             = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES'

itipo_informacion = 1 # 1-dato nuevo (analisis laboratorio)  2-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'


### PROCESADO ###

# Listado de archivos disponibles
from os import listdir
from os.path import isfile, join
listado_archivos = [f for f in listdir(directorio_datos) if isfile(join(directorio_datos, f))]

for iarchivo in range(len(listado_archivos)):

    nombre_archivo = directorio_datos + '/' + listado_archivos[iarchivo]
   
    print('Procesando la informacion correspondiente al año ',nombre_archivo[-9:-5])
 
    print('Leyendo los datos contenidos en el archivo excel')
    datos_radiales = FUNCIONES_INSERCION.lectura_datos_radiales(nombre_archivo,direccion_host,base_datos,usuario,contrasena,puerto)
    
    # Realiza un control de calidad primario a los datos importados   
    print('Realizando control de calidad')
    datos_radiales_corregido = FUNCIONES_INSERCION.control_calidad(datos_radiales,archivo_variables_base_datos)  
     
    # Recupera el identificador del programa de muestreo
    id_programa = FUNCIONES_INSERCION.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)
    
    # Introduce los datos en la base de datos
    print('Introduciendo los datos en la base de datos')
    FUNCIONES_INSERCION.inserta_datos(datos_radiales_corregido,min_dist,programa_muestreo,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)
          
    # Actualiza estado
    print('Actualizando el estado de los procesos')
    FUNCIONES_INSERCION.actualiza_estado(datos_radiales_corregido,id_programa,programa_muestreo,itipo_informacion,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto)

    print('Procesado del año ', nombre_archivo[-9:-5], ' terminado')
    
print('Fin del procesado de todos los datos disponibles')
  
    
