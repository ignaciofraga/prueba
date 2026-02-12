# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""

import FUNCIONES_LECTURA
import FUNCIONES_PROCESADO
import pandas
pandas.options.mode.chained_assignment = None
import datetime
import pandas.io.sql as psql
from sqlalchemy import create_engine


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_muestreo = 'RADIAL CANTABRICO'



itipo_informacion = 1 # 1-dato nuevo (analisis laboratorio)  2-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'


# Define la fecha actual
fecha_actualizacion = datetime.date.today()

### PROCESADO ###

# Carga de informacion disponible
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn             = create_engine(con_engine)
tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
tabla_datos      = psql.read_sql('SELECT * FROM datos_discretos', conn)
tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
tabla_variables  = psql.read_sql('SELECT * FROM variables_procesado', conn)
conn.dispose() 

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)


# Lectura de hoja excel con los resultados
print('Leyendo los datos contenidos en el archivo excel')
nombre_archivo = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADCAN/RADCAN.xlsx'
datos_radcan   = FUNCIONES_LECTURA.lectura_datos_radcan(nombre_archivo)

           
# Realiza un control de calidad primario a los datos importados   
print('Realizando control de calidad')
datos_radcan_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_radcan) 

# Encuentra la estación asociada a cada registro
print('Asignando la estación correspondiente a cada medida')
datos_radcan_corregido = FUNCIONES_PROCESADO.evalua_estaciones(datos_radcan_corregido,id_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_muestreos)


# Encuentra las salidas al mar correspondientes
tipo_salida = 'MENSUAL'   
datos_radcan_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos_radcan_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_salidas,tabla_muestreos)
 
# Encuentra el identificador asociado a cada registro
print('Asignando el registro correspondiente a cada medida')
datos_radcan_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_radcan_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)
   
# # # # # Introduce los datos en la base de datos
print('Introduciendo los datos en la base de datos')
texto_insercion = FUNCIONES_PROCESADO.inserta_datos(datos_radcan_corregido,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos,tabla_muestreos)



