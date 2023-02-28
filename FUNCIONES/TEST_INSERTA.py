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
programa_muestreo = 'PELACUS'
id_config_sup     = 1
id_config_per     = 1

# Rutas de los archivos a importar  
archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx' 

# Tipo de información a introducir
itipo_informacion = 2 # 1-nuevo muestreo 2-dato nuevo (analisis laboratorio)  3-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'

fecha_actualizacion = datetime.date.today()

###### PROCESADO ########

# Carga los datos
print('Leyendo los datos contenidos en la excel')
datos_pelacus = FUNCIONES_LECTURA.lectura_datos_pelacus(archivo_datos)  
 
# Realiza un control de calidad primario a los datos importados   
print('Realizando control de calidad')
datos_pelacus_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_pelacus,direccion_host,base_datos,usuario,contrasena,puerto)  

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# Encuentra la estación asociada a cada registro
print('Identificando la estación correspondiente a cada medida')
datos_pelacus_corregido = FUNCIONES_PROCESADO.evalua_estaciones(datos_pelacus_corregido,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# Asigna el identificador de salida al mar correspondiente
tipo_salida = 'ANUAL'
datos_pelacus_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos_pelacus_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)

# Encuentra el identificador asociado a cada registro
print('Identificando el registro correspondiente a cada medida')
datos_pelacus_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_pelacus_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)

print('Insertando los datos en la base de datos')
FUNCIONES_PROCESADO.inserta_datos(datos_pelacus_corregido,'fisica',direccion_host,base_datos,usuario,contrasena,puerto)
FUNCIONES_PROCESADO.inserta_datos(datos_pelacus_corregido,'bgq',direccion_host,base_datos,usuario,contrasena,puerto)
# from sqlalchemy import create_engine
# import pandas.io.sql as psql
# import psycopg2
# datos_fisica = datos_pelacus_corregido

# #datos_recorte = datos[['']]

# # Recupera la tabla con los registros de muestreos físicos
# con_engine         = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn_psql          = create_engine(con_engine)
# tabla_variables    = psql.read_sql('SELECT * FROM variables_procesado', conn_psql)  
# conn_psql.dispose()
    
# # Lee las variables de cada tipo a utilizar en el control de calidad
# variables_muestreo = [x for x in tabla_variables['parametros_muestreo'] if str(x) != 'None']
# variables_fisicas  = [x for x in tabla_variables['variables_fisicas'] if str(x) != 'None']    
# variables_biogeoquimicas  = [x for x in tabla_variables['variables_biogeoquimicas'] if str(x) != 'None'] 

# # Busca qué variables están incluidas en los datos a importar
# listado_variables_datos   = datos_fisica.columns.tolist()
# listado_variables_comunes = list(set(listado_variables_datos).intersection(variables_fisicas))
# listado_adicional         = ['muestreo'] + listado_variables_comunes

# # Recupera la tabla con los registros de muestreos físicos
# con_engine                = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn_psql                 = create_engine(con_engine)
# tabla_registros_fisica    = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn_psql)

# # Genera un dataframe solo con las variales fisicas de los datos a importar 
# datos_fisica = datos_fisica.rename(columns={"id_muestreo": "muestreo"})

# # # Si no existe ningún registro en la base de datos, introducir todos los datos disponibles
# if tabla_registros_fisica.shape[0] == 0:
#     datos_fisica.set_index('muestreo',drop=True,append=False,inplace=True)
#     datos_fisica.to_sql('datos_discretos_fisica', conn_psql,if_exists='append')
    
# # En caso contrario, comprobar qué parte de la información está en la base de datos
# else: 
    
#     for idato in range(datos_fisica.shape[0]): # Dataframe con la interseccion de los datos nuevos y los disponibles en la base de datos, a partir de la variable muestreo
     
#         df_temp  = tabla_registros_fisica[(tabla_registros_fisica['muestreo']==datos_fisica['muestreo'].iloc[idato])] 
        
#         if df_temp.shape[0]>0:  # Muestreo ya incluido en la base de datos
        
#             muestreo = df_temp['muestreo'].iloc[0]
            
#             for ivariable in range(len(listado_variables_comunes)): # Reemplazar las variables disponibles en el muestreo correspondiente
                    
#                 tabla_registros_fisica[listado_variables_comunes[ivariable]][tabla_registros_fisica['muestreo']==int(muestreo)] = datos_fisica[listado_variables_comunes[ivariable]][datos_fisica['muestreo']==int(muestreo)]

        
#         else: # Nuevo muestreo
                   
#             df_add = datos_fisica[datos_fisica['muestreo']==datos_fisica['muestreo'].iloc[idato]] # Genero un dataframe con cada línea de datos a añadir

#             df_add = df_add[listado_adicional] # Recorto para que tenga sólo las variables a añadir
        
#             tabla_registros_fisica = pandas.concat([tabla_registros_fisica, df_add]) # Combino ambos dataframes
        
#     tabla_registros_fisica.set_index('muestreo',drop=True,append=False,inplace=True)
    
#     # borra los registros existentes en la tabla (no la tabla en sí, para no perder tipos de datos y referencias)
#     conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
#     cursor = conn.cursor()
#     instruccion_sql = "TRUNCATE datos_discretos_fisica;"
#     cursor.execute(instruccion_sql)
#     conn.commit()
#     cursor.close()
#     conn.close() 
    
#     # Inserta el dataframe resultante en la base de datos 
#     tabla_registros_fisica.to_sql('datos_discretos_fisica', conn_psql,if_exists='append')


# conn_psql.dispose() # Cierra la conexión con la base de datos 





# # Introduce los datos en la base de datos
# print('Introduciendo los datos en la base de datos')
# 
# FUNCIONES_PROCESADO.inserta_datos_biogeoquimica(datos_pelacus_corregido,direccion_host,base_datos,usuario,contrasena,puerto)


# FUNCIONES_AUXILIARES.actualiza_estado(datos_pelacus_corregido,id_programa,programa_muestreo,fecha_actualizacion,email_contacto,itipo_informacion,direccion_host,base_datos,usuario,contrasena,puerto)


print('Procesado terminado')





