# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 08:15:20 2022

@author: ifraga
"""

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

import numpy
import pandas
import datetime
import numpy
import pandas
import datetime
from pyproj import Proj
import math
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine
import json
import re

import FUNCIONES_PROCESADO
import FUNCIONES_LECTURA


programa_seleccionado = 'RADIAL CORUÑA'
tipo_salida           = 'MENSUAL'

archivo_datos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/PRUEBA_WEB/prueba/EJEMPLOS/EXCEL_ENTRADA.xlsx'

# Lectura del archivo con los resultados del AA
df_datos_importacion =pandas.read_excel(archivo_datos)

# corrige el formato de las fechas
for idato in range(df_datos_importacion.shape[0]):
    df_datos_importacion['fecha_muestreo'][idato] = (df_datos_importacion['fecha_muestreo'][idato]).date()
    if df_datos_importacion['fecha_muestreo'][idato]:
        df_datos_importacion['hora_muestreo'][idato] = datetime.datetime.strptime(df_datos_importacion['hora_muestreo'][idato], '%H:%M:%S').time()

# Realiza un control de calidad primario a los datos importados   
datos_corregidos,textos_aviso   = FUNCIONES_PROCESADO.control_calidad(df_datos_importacion,direccion_host,base_datos,usuario,contrasena,puerto)  

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)


#with st.spinner('Asignando la estación y salida al mar de cada medida'):
# Encuentra la estación asociada a cada registro
datos_corregidos = FUNCIONES_PROCESADO.evalua_estaciones(datos_corregidos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# Encuentra las salidas al mar correspondientes  
datos_corregidos = FUNCIONES_PROCESADO.evalua_salidas(datos_corregidos,id_programa,programa_seleccionado,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
 
datos = datos_corregidos

import streamlit as st

# Recupera la tabla con los registros de los muestreos
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
   
datos['id_muestreo_temp']  = numpy.zeros(datos.shape[0],dtype=int)

# si no hay ningun valor en la tabla de registro, meter directamente todos los datos registrados
if tabla_muestreos.shape[0] == 0:

    # genera un dataframe con las variables que interesa introducir en la base de datos
    exporta_registros                    = datos[['id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','configuracion_perfilador','configuracion_superficie']]
    # añade el indice de cada registro
    indices_registros                    = numpy.arange(1,(exporta_registros.shape[0]+1))    
    exporta_registros['id_muestreo']     = indices_registros
    # renombra la columna con información de la estación muestreada
    exporta_registros                    = exporta_registros.rename(columns={"id_estacion_temp":"estacion",'id_salida':'salida_mar'})
    # # añade el nombre del muestreo
    exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
    for idato in range(exporta_registros.shape[0]):    
        nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'][idato]]['nombre_estacion'].iloc[0]
        
        nombre_muestreo     = abreviatura_programa + '_' + datos['fecha_muestreo'][idato].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
        if datos['num_cast'][idato] is not None:
            nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'][idato]))
        if datos['botella'][idato] is not None:
            nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'][idato]))                
            
        exporta_registros['nombre_muestreo'][idato]  = nombre_muestreo

        datos['id_muestreo_temp'] [idato]            = idato + 1
        
        
    # Inserta en base de datos        
    exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
    exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append') 

# En caso contrario hay que ver registro a registro, si ya está incluido en la base de datos
else:

    ultimo_registro_bd         = max(tabla_muestreos['id_muestreo'])
    datos['io_nuevo_muestreo'] = numpy.ones(datos.shape[0],dtype=int)

    for idato in range(datos.shape[0]):

        for idato_existente in range(tabla_muestreos.shape[0]):
            
            if tabla_muestreos['hora_muestreo'][idato_existente] is not None and datos['hora_muestreo'][idato] is not None:
                # Registro ya incluido, recuperar el identificador
                if tabla_muestreos['estacion'][idato_existente] == datos['id_estacion_temp'][idato] and tabla_muestreos['fecha_muestreo'][idato_existente] == datos['fecha_muestreo'][idato] and  tabla_muestreos['hora_muestreo'][idato_existente] == datos['hora_muestreo'][idato] and  tabla_muestreos['presion_ctd'][idato_existente] == datos['presion_ctd'][idato] and  tabla_muestreos['configuracion_perfilador'][idato_existente] == datos['configuracion_perfilador'][idato] and  tabla_muestreos['configuracion_superficie'][idato_existente] == datos['configuracion_superficie'][idato]:
                    datos['id_muestreo_temp'] [idato] =  tabla_muestreos['id_muestreo'][idato_existente]    
                    datos['io_nuevo_muestreo'][idato] = 0
                    st.text('jol')
            else:  
                if tabla_muestreos['estacion'][idato_existente] == datos['id_estacion_temp'][idato] and tabla_muestreos['fecha_muestreo'][idato_existente] == datos['fecha_muestreo'][idato] and   tabla_muestreos['presion_ctd'][idato_existente] == datos['presion_ctd'][idato] and  tabla_muestreos['configuracion_perfilador'][idato_existente] == datos['configuracion_perfilador'][idato] and  tabla_muestreos['configuracion_superficie'][idato_existente] == datos['configuracion_superficie'][idato]:
                    datos['id_muestreo_temp'] [idato] =  tabla_muestreos['id_muestreo'][idato_existente]    
                    datos['io_nuevo_muestreo'][idato] = 0     
                    st.text('ole')
            
        # Nuevo registro
        if datos['io_nuevo_muestreo'][idato] == 1:
            # Asigna el identificador (siguiente al máximo disponible)
            ultimo_registro_bd                = ultimo_registro_bd + 1
            datos['id_muestreo_temp'][idato]  = ultimo_registro_bd  
 
    st.text(datos['io_nuevo_muestreo'])
    
    if numpy.count_nonzero(datos['io_nuevo_muestreo']) > 0:
    
        # Genera un dataframe sólo con los valores nuevos, a incluir (io_nuevo_muestreo = 1)
        nuevos_muestreos  = datos[datos['io_nuevo_muestreo']==1]
        # Mantén sólo las columnas que interesan
        exporta_registros = nuevos_muestreos[['id_muestreo_temp','id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','configuracion_perfilador','configuracion_superficie']]
                    
        # Cambia el nombre de la columna de estaciones
        exporta_registros = exporta_registros.rename(columns={"id_estacion_temp":"estacion","id_muestreo_temp":"id_muestreo",'id_salida':'salida_mar'})
        # Indice temporal
        exporta_registros['indice_temporal'] = numpy.arange(0,exporta_registros.shape[0])
        exporta_registros.set_index('indice_temporal',drop=True,append=False,inplace=True)
        # Añade el nombre del muestreo
        exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
        for idato in range(exporta_registros.shape[0]):    
            nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'][idato]]['nombre_estacion'].iloc[0]
          
            nombre_muestreo     = abreviatura_programa + '_' + datos['fecha_muestreo'][idato].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
            if datos['num_cast'][idato] is not None:
                nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'][idato]))
            if datos['botella'][idato] is not None:
                nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'][idato]))                
             
            exporta_registros['nombre_muestreo'][idato]  = nombre_muestreo


        # # Inserta el dataframe resultante en la base de datos 
        exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
        exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append')    

conn_psql.dispose() # Cierra la conexión con la base de datos 










# Encuentra el identificador asociado a cada registro
#with st.spinner('Asignando el registro correspondiente a cada medida'):
#datos_corregidos = FUNCIONES_PROCESADO.evalua_registros(datos_corregidos,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
   
# Introduce los datos en la base de datos
#with st.spinner('Intoduciendo la información en la base de datos'):

# FUNCIONES_PROCESADO.inserta_datos_fisica(datos_corregidos,direccion_host,base_datos,usuario,contrasena,puerto)

# FUNCIONES_PROCESADO.inserta_datos_biogeoquimica(datos_corregidos,direccion_host,base_datos,usuario,contrasena,puerto)


    
    

