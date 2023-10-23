# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""







import FUNCIONES_LECTURA
import FUNCIONES_PROCESADO
import INSERTA_DATOS_RADIALES_HISTORICO
import pandas
pandas.options.mode.chained_assignment = None
import datetime



# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'
programa_muestreo = 'RADIAL CORUÑA'

# # # # DATOS HISTORICOS (1988 - 2012)
# nombre_archivo    = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/HISTORICO/HISTORICO_MODIFICADO.xlsx'
# print('Añadiendo los datos históricos')   
# INSERTA_DATOS_RADIALES_HISTORICO.inserta_radiales_historico(nombre_archivo,base_datos,usuario,contrasena,puerto,direccion_host,programa_muestreo)



# # # RADIALES 2013-2020
# directorio_datos           = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES'
# # Listado de archivos disponibles
# from os import listdir
# from os.path import isfile, join
# listado_archivos = [f for f in listdir(directorio_datos) if isfile(join(directorio_datos, f))]

# for iarchivo in range(len(listado_archivos)):
# # #for iarchivo in range(0,1):

#     nombre_archivo = directorio_datos + '/' + listado_archivos[iarchivo]

# #nombre_archivo = directorio_datos + '/RADIAL_BTL_COR_2020.xlsx'

#     print('Procesando la informacion correspondiente al año ',nombre_archivo[-9:-5])
    
#     print('Leyendo los datos contenidos en el archivo excel')
#     datos_radiales = FUNCIONES_LECTURA.lectura_datos_radiales(nombre_archivo,direccion_host,base_datos,usuario,contrasena,puerto)
        
#     # Realiza un control de calidad primario a los datos importados   
#     print('Realizando control de calidad')
#     datos_radiales_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_radiales)  
    
#     datos_radiales_corregido = datos_radiales_corregido[datos_radiales_corregido['presion_ctd'].notna()] 
#     datos_radiales_corregido = datos_radiales_corregido[datos_radiales_corregido['fecha_muestreo'].notna()] 
    
#     #datos_radiales_corregido['temperatura_ctd_qf'] = 
    
#     # Recupera el identificador del programa de muestreo
#     id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)
    
#     # Encuentra la estación asociada a cada registro
#     print('Asignando la estación correspondiente a cada medida')
#     datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_estaciones(datos_radiales_corregido,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)
    
#     # Encuentra las salidas al mar correspondientes
#     tipo_salida = 'MENSUAL'   
#     datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos_radiales_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
     
#     # Encuentra el identificador asociado a cada registro
#     print('Asignando el registro correspondiente a cada medida')
#     datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_radiales_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
       
#     # # # # # Introduce los datos en la base de datos
#     print('Introduciendo los datos en la base de datos')
#     FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)
    


# # DATOS DEL 21-23
# # # Parámetros
# anhos = [2021]
# ruta_archivos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/MENSUALES/Procesados'
# import INSERTA_DATOS_PERFILES
# for ianho in range(len(anhos)):
#     anho = anhos[ianho]
#     INSERTA_DATOS_PERFILES.inserta_radiales_historico(ruta_archivos,anho,programa_muestreo,base_datos,usuario,contrasena,puerto,direccion_host)       



# ####################
# # Nutrientes 21-22 #
# ####################
nombre_archivo    = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/SOLO_NUTRIENTES/NUTRIENTES-21-22.xlsx'

datos_radiales = pandas.read_excel(nombre_archivo)
datos_radiales_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_radiales)  


datos_radiales_corregido['nitrato']    = datos_radiales_corregido['ton'] - datos_radiales_corregido['nitrito']
datos_radiales_corregido['nitrato_qf'] = 9
for idato in range(datos_radiales_corregido.shape[0]):
    if datos_radiales_corregido['ton_qf'].iloc[idato] == 2 and datos_radiales_corregido['nitrito_qf'].iloc[idato]:
        datos_radiales_corregido['nitrato_qf'].iloc[idato] = 2 

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# Encuentra la salida correspondinte a cada dato
tipo_salida = 'MENSUAL'   
datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos_radiales_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
     

# Encuentra el identificador asociado a cada registro
print('Asignando el registro correspondiente a cada medida')
datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_radiales_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
 
print('Introduciendo los datos en la base de datos')
FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)

  

# ########################################
# # Clorofilas, prod_primaria, cop y nop #
# ########################################

# fecha_umbral = datetime.date(2018,1,1)
# datos_radiales = INSERTA_DATOS_RADIALES_HISTORICO.recupera_id(fecha_umbral,usuario,contrasena,direccion_host,puerto,base_datos)

# # Recupera el identificador del programa de muestreo
# id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra el identificador asociado a cada registro
# print('Asignando el registro correspondiente a cada medida')
# datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_radiales,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
 
# print('Introduciendo los datos en la base de datos')
# FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)
# FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)
  