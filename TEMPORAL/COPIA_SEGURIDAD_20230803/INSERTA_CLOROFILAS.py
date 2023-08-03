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
import pandas.io.sql as psql
from sqlalchemy import create_engine
import psycopg2
import numpy

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_muestreo = 'RADIAL CORUÑA'
tipo_salida       = 'MENSUAL'

# Rutas de los archivos a importar  
#archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx' 
archivo_datos                ='C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/CLOROFILAS/Clorofila a,b,c Radial 2018.xlsx'

# Tipo de información a introducir
itipo_informacion = 2 # 1-nuevo muestreo 2-dato nuevo (analisis laboratorio)  3-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'

# Recupera la tabla con los registros de los muestreos
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
df_muestreos     = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
conn_psql.dispose()   

###### PROCESADO ########

# Importa el .xlsx
datos_radiales = pandas.read_excel(archivo_datos, 'total',index_col=None,skiprows=0)

# Convierte las fechas de DATE a formato correcto
datos_radiales['fecha'] =  pandas.to_datetime(datos_radiales['fecha'], format='%Y%m%d').dt.date
  
# Define una columna índice
indices_dataframe         = numpy.arange(0,datos_radiales.shape[0],1,dtype=int)
datos_radiales['id_temp'] = indices_dataframe
datos_radiales.set_index('id_temp',drop=True,append=False,inplace=True)


datos_radiales['prof_bd'] = [None]*datos_radiales.shape[0]
datos_radiales['nombre_muestreo'] = [None]*datos_radiales.shape[0]

for idato in range(datos_radiales.shape[0]):
  
    if datos_radiales['estacion'].iloc[idato] == 'E2CO':
        datos_radiales['estacion'].iloc[idato] = 1
    if datos_radiales['estacion'].iloc[idato] == 'E3ACO':       
        datos_radiales['estacion'].iloc[idato] = 2
    if datos_radiales['estacion'].iloc[idato] == 'E3BCO':       
        datos_radiales['estacion'].iloc[idato] = 4
    if datos_radiales['estacion'].iloc[idato] == 'E3CCO':       
        datos_radiales['estacion'].iloc[idato] = 3
    if datos_radiales['estacion'].iloc[idato] == 'E4CO':       
        datos_radiales['estacion'].iloc[idato] = 5
        
    
                
    df_temp = df_muestreos[(df_muestreos['fecha_muestreo']==datos_radiales['fecha'].iloc[idato]) & (df_muestreos['estacion']==datos_radiales['estacion'].iloc[idato])]


    dif_profs     = numpy.asarray(abs(df_temp['presion_ctd'] - datos_radiales['prof'].iloc[idato]))
    indice_posicion = numpy.argmin(dif_profs)

    datos_radiales['prof_bd'].iloc[idato] = df_temp['presion_ctd'].iloc[indice_posicion]
    datos_radiales['nombre_muestreo'].iloc[idato] = df_temp['nombre_muestreo'].iloc[indice_posicion]




datos = datos_radiales[datos_radiales['nombre_muestreo'].notna()]
datos = datos.rename(columns={"Cl_a": "clorofila_a", "Cl_b": "clorofila_b","Cl_c":"clorofila_c"})        					
datos_recorte = datos[['nombre_muestreo','clorofila_a','clorofila_b','clorofila_c']]

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)
 
# Encuentra el identificador asociado a cada registro
print('Asignando el registro correspondiente a cada medida')
datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_recorte,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
   
# # # # # Introduce los datos en la base de datos
print('Introduciendo los datos en la base de datos')
FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)












# con_engine      = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn            = create_engine(con_engine)
# df_programas              = psql.read_sql('SELECT * FROM programas', conn)
# variables_bd              = psql.read_sql('SELECT * FROM variables_procesado', conn)
# conn.dispose() 



# df_datos_importacion  = pandas.read_excel(archivo_datos) 

# # Identifica las variables que contiene el archivo
# variables_archivo = df_datos_importacion.columns.tolist()
# variables_fisica  = list(set(variables_bd['variables_fisicas']).intersection(variables_archivo))
# variables_bgq     = list(set(variables_bd['variables_biogeoquimicas']).intersection(variables_archivo))
        
        
# # Corrige el formato de las fechas
# for idato in range(df_datos_importacion.shape[0]):
#     df_datos_importacion['fecha_muestreo'][idato] = (df_datos_importacion['fecha_muestreo'][idato]).date()           
#     if df_datos_importacion['fecha_muestreo'][idato]:
#         if 'hora_muestreo' in variables_archivo and isinstance(df_datos_importacion['hora_muestreo'][idato], str):
#             df_datos_importacion['hora_muestreo'][idato] = datetime.datetime.strptime(df_datos_importacion['hora_muestreo'][idato], '%H:%M:%S').time()

# # Cambia el nombre del identificador 
# try:
#     df_datos_importacion = df_datos_importacion.rename(columns={"ID": "id_externo"})
# except:
#     texto_aviso = "Los datos importados no contienen identificador."
#     #st.warning(texto_aviso, icon="⚠️")



# # Realiza un control de calidad primario a los datos importados   
# datos_corregidos,textos_aviso   = FUNCIONES_PROCESADO.control_calidad(df_datos_importacion,direccion_host,base_datos,usuario,contrasena,puerto)  

# # Recupera el identificador del programa de muestreo
# id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)



# # Encuentra la estación asociada a cada registro
# datos_corregidos = FUNCIONES_PROCESADO.evalua_estaciones(datos_corregidos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra las salidas al mar correspondientes  
# programa_seleccionado = 'MUESTRAS ACUARIO'
# datos_corregidos = FUNCIONES_PROCESADO.evalua_salidas(datos_corregidos,id_programa,programa_seleccionado,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
 
# # Encuentra el identificador asociado a cada registro
# datos_corregidos = FUNCIONES_PROCESADO.evalua_registros(datos_corregidos,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
     
# # # Añade datos físicos
# # if len(variables_fisica)>0:
        
# #     FUNCIONES_PROCESADO.inserta_datos(datos_corregidos,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)

# # # Añade datos biogeoquímicos
# # if len(variables_bgq)>0:

# FUNCIONES_PROCESADO.inserta_datos(datos_corregidos,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)

        
# # texto_exito = 'Datos del archivo ' + archivo_datos.name + ' añadidos correctamente a la base de datos'
# # #st.success(texto_exito)


