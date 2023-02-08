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


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_muestreo = 'RADIAL CANTABRICO'

#archivo_variables_base_datos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/VARIABLES.xlsx'  
directorio_datos             = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADCAN'

itipo_informacion = 1 # 1-dato nuevo (analisis laboratorio)  2-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'


# Define la fecha actual
fecha_actualizacion = datetime.date.today()

### PROCESADO ###

# Listado de archivos disponibles

listado_archivos = ['RADCAN_2020.xlsx','RADCAN_2019.xlsx','RADCAN_2012-2013-2014.xlsx']
#listado_archivos = ['RADCAN_2019.xlsx']

for iarchivo in range(len(listado_archivos)):
#for iarchivo in range(3,5):

    nombre_archivo = directorio_datos + '/' + listado_archivos[iarchivo]
    
    print('Procesando la informacion correspondiente al año ',nombre_archivo[-9:-5])

    print('Leyendo los datos contenidos en el archivo excel')
    
    datos_radiales = pandas.read_excel(nombre_archivo, 'datos',dtype={'hora_muestreo': datetime.time})
    for idato in range(datos_radiales.shape[0]):
        datos_radiales['fecha_muestreo'][idato] = (datos_radiales['fecha_muestreo'][idato]).date()
        
    # Añade el identificador de la configuración del perfilador y la superficie (darle una vuelta a esto)
    datos_radiales['configuracion_perfilador'] = [None]*datos_radiales.shape[0]
    datos_radiales['configuracion_superficie'] = [None]*datos_radiales.shape[0]
    
    
    
    #datos_radiales,texto_error = FUNCIONES_LECTURA.lectura_datos_estadillo(nombre_archivo,nombre_archivo)
        
    # Realiza un control de calidad primario a los datos importados   
    print('Realizando control de calidad')
    datos_radiales_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_radiales,direccion_host,base_datos,usuario,contrasena,puerto)  

    # Recupera el identificador del programa de muestreo
    id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)
    
    # Encuentra la estación asociada a cada registro
    print('Asignando la estación correspondiente a cada medida')
    datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_estaciones(datos_radiales_corregido,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

    # Encuentra las salidas al mar correspondientes
    tipo_salida = 'MENSUAL'   
    datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos_radiales_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
 
    # Encuentra el identificador asociado a cada registro
    print('Asignando el registro correspondiente a cada medida')
    datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_radiales_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
   
    # # # # # Introduce los datos en la base de datos
    print('Introduciendo los datos en la base de datos')
    
    FUNCIONES_PROCESADO.inserta_datos_fisica(datos_radiales_corregido,direccion_host,base_datos,usuario,contrasena,puerto)

    FUNCIONES_PROCESADO.inserta_datos_biogeoquimica(datos_radiales_corregido,direccion_host,base_datos,usuario,contrasena,puerto)


# #     # # Actualiza estado
# #     # print('Actualizando el estado de los procesos')
#     FUNCIONES_AUXILIARES.actualiza_estado(datos_radiales_corregido,id_programa,programa_muestreo,fecha_actualizacion,email_contacto,itipo_informacion,direccion_host,base_datos,usuario,contrasena,puerto)
                         

#     # print('Procesado del año ', nombre_archivo[-9:-5], ' terminado')
    
# print('Fin del procesado de todos los datos disponibles')
  
    
