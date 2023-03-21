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

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_seleccionado = 'RADIAL CANTABRICO'
tipo_salida           = 'MENSUAL'
id_config_sup     = 1
id_config_per     = 1

# Rutas de los archivos a importar  
#archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx' 
archivo_datos                ='C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/PRUEBAS STREAMLIT/BOTELLAS_RADCAN_COSTERO_2019.xlsx'

# Tipo de información a introducir
itipo_informacion = 2 # 1-nuevo muestreo 2-dato nuevo (analisis laboratorio)  3-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'

fecha_actualizacion = datetime.date.today()

###### PROCESADO ########


con_engine      = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn            = create_engine(con_engine)
df_programas    = psql.read_sql('SELECT * FROM programas', conn)
variables_bd    = psql.read_sql('SELECT * FROM variables_procesado', conn)
conn.dispose() 


df_datos_importacion  = pandas.read_excel(archivo_datos) 

# corrige el formato de las fechas
for idato in range(df_datos_importacion.shape[0]):
    df_datos_importacion['fecha_muestreo'][idato] = (df_datos_importacion['fecha_muestreo'][idato]).date()           
    if df_datos_importacion['fecha_muestreo'][idato]:
        if isinstance(df_datos_importacion['hora_muestreo'][idato], str):
            df_datos_importacion['hora_muestreo'][idato] = datetime.datetime.strptime(df_datos_importacion['hora_muestreo'][idato], '%H:%M:%S').time()

df_datos_importacion = df_datos_importacion.rename(columns={"ID": "id_externo"})

# Identifica las variables que contiene el archivo
variables_archivo = df_datos_importacion.columns.tolist()
variables_fisica  = list(set(variables_bd['variables_fisicas']).intersection(variables_archivo))
variables_bgq     = list(set(variables_bd['variables_biogeoquimicas']).intersection(variables_archivo))

# Realiza un control de calidad primario a los datos importados   
datos_corregidos,textos_aviso   = FUNCIONES_PROCESADO.control_calidad(df_datos_importacion,direccion_host,base_datos,usuario,contrasena,puerto)  

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)



# Encuentra la estación asociada a cada registro
datos_corregidos = FUNCIONES_PROCESADO.evalua_estaciones(datos_corregidos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# Encuentra las salidas al mar correspondientes  
datos_corregidos = FUNCIONES_PROCESADO.evalua_salidas(datos_corregidos,id_programa,programa_seleccionado,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
 

datos_corregidos = FUNCIONES_PROCESADO.evalua_registros(datos_corregidos,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)


# Introducir los valores en la base de datos
conn   = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()  
   
instruccion_sql = "UPDATE muestreos_discretos SET salida_mar= %s WHERE muestreo = %s;"

for idato in range(datos_corregidos.shape[0]):
    
    cursor.execute(instruccion_sql, (int(datos_corregidos['id_salida'].iloc[idato]),int(datos_corregidos['muestreo'].iloc[idato])))
    conn.commit() 

cursor.close()
conn.close()   
    

# datos = datos_corregidos
# import numpy


# # Recupera la tabla con los registros de los muestreos
# con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn_psql        = create_engine(con_engine)
# tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
# tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
# tabla_variables  = psql.read_sql('SELECT * FROM variables_procesado', conn_psql)
   
# datos['muestreo']  = numpy.zeros(datos.shape[0],dtype=int)

# # si no hay ningun valor en la tabla de registro, meter directamente todos los datos registrados
# if tabla_muestreos.shape[0] == 0:

#     # genera un dataframe con las variables que interesa introducir en la base de datos
#     exporta_registros                    = datos[['id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','latitud','longitud']]
    
    
    
#     # añade el indice de cada registro
#     indices_registros                    = numpy.arange(1,(exporta_registros.shape[0]+1))    
#     exporta_registros['muestreo']     = indices_registros
#     # renombra la columna con información de la estación muestreada
#     exporta_registros                    = exporta_registros.rename(columns={"id_estacion_temp":"estacion",'id_salida':'salida_mar','latitud':'latitud_muestreo','longitud':'longitud_muestreo'})
#     # # añade el nombre del muestreo
#     exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
#     for idato in range(exporta_registros.shape[0]):    
#         nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'].iloc[idato]]['nombre_estacion'].iloc[0]
        
#         nombre_muestreo     = abreviatura_programa + '_' + datos['fecha_muestreo'].iloc[idato].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
#         if datos['num_cast'].iloc[idato] is not None:
#             nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'].iloc[idato]))
#         else:
#             nombre_muestreo = nombre_muestreo + '_C1' 
            
#         if datos['botella'].iloc[idato] is not None:
#             nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'].iloc[idato])) 
#         else:
#             if datos['prof_referencia'].iloc[idato] is not None: 
#                 nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['prof_referencia'].iloc[idato]))
#             else:
#                 nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['presion_ctd'].iloc[idato])) 
            
#         exporta_registros['nombre_muestreo'].iloc[idato]  = nombre_muestreo

#         datos['muestreo'].iloc[idato]                 = idato + 1
        
        
#     # Inserta en base de datos        
#     exporta_registros.set_index('muestreo',drop=True,append=False,inplace=True)
#     exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append') 

# # En caso contrario hay que ver registro a registro, si ya está incluido en la base de datos
# else:

#     ultimo_registro_bd         = max(tabla_muestreos['muestreo'])
#     datos['io_nuevo_muestreo'] = numpy.ones(datos.shape[0],dtype=int)

#     for idato in range(datos.shape[0]):

#         if datos['botella'].iloc[idato] is not None:        
#             if datos['hora_muestreo'].iloc[idato] is not None:          
#                 df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['botella']==datos['botella'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) & (tabla_muestreos['hora_muestreo']==datos['hora_muestreo'].iloc[idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]

#             else:
#                 df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['botella']==datos['botella'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]
#         else:
#             if datos['hora_muestreo'].iloc[idato] is not None:          
#                 df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) & (tabla_muestreos['hora_muestreo']==datos['hora_muestreo'].iloc[idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]

#             else:
#                 df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]
           
        
#         if df_temp.shape[0]> 0:
#             datos['muestreo'].iloc[idato]          = df_temp['muestreo'].iloc[0]    
#             datos['io_nuevo_muestreo'].iloc[idato] = 0
            
#         else:
#             datos['io_nuevo_muestreo'].iloc[idato] = 1
#             ultimo_registro_bd                     = ultimo_registro_bd + 1
#             datos['muestreo'].iloc[idato]       = ultimo_registro_bd 
        
    
#     if numpy.count_nonzero(datos['io_nuevo_muestreo']) > 0:
    
#         # Genera un dataframe sólo con los valores nuevos, a incluir (io_nuevo_muestreo = 1)
#         nuevos_muestreos  = datos[datos['io_nuevo_muestreo']==1]
        
#         # Mantén sólo las columnas que interesan
#         variables_bd  = [x for x in tabla_variables['parametros_muestreo'] if str(x) != 'None']
        
#         # Busca qué variables están incluidas en los datos a importar
#         listado_variables_datos   = datos.columns.tolist()
#         listado_variables_comunes = list(set(listado_variables_datos).intersection(variables_bd))
#         listado_adicional         = ['muestreo','id_estacion_temp'] + listado_variables_comunes
#         exporta_registros = nuevos_muestreos[listado_adicional]
                    
#         # Cambia el nombre de la columna de estaciones
#         exporta_registros = exporta_registros.rename(columns={"id_estacion_temp":"estacion",'id_salida':'salida_mar','latitud':'latitud_muestreo','longitud':'longitud_muestreo'})
#         # Indice temporal
#         exporta_registros['indice_temporal'] = numpy.arange(0,exporta_registros.shape[0])
#         exporta_registros.set_index('indice_temporal',drop=True,append=False,inplace=True)
#         # Añade el nombre del muestreo
#         exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
#         for idato in range(exporta_registros.shape[0]):    
#             nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'].iloc[idato]]['nombre_estacion'].iloc[0]
          
#             nombre_muestreo     = abreviatura_programa + '_' + datos['fecha_muestreo'].iloc[idato].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
#             if datos['num_cast'].iloc[idato] is not None:
#                 nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'].iloc[idato]))
#             else:
#                 nombre_muestreo = nombre_muestreo + '_C1'     
            
#             if datos['botella'].iloc[idato] is not None:
#                 nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'].iloc[idato])) 
#             else:
#                 if datos['prof_referencia'].iloc[idato] is not None: 
#                     nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['prof_referencia'].iloc[idato]))
#                 else:
#                     nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['presion_ctd'].iloc[idato])) 
             
#             exporta_registros['nombre_muestreo'].iloc[idato]  = nombre_muestreo
    
#         # # Inserta el dataframe resultante en la base de datos 
#         exporta_registros.set_index('muestreo',drop=True,append=False,inplace=True)
#         exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append')    

# conn_psql.dispose() # Cierra la conexión con la base de datos  

   




# # Carga los datos
# print('Leyendo los datos contenidos en la excel')
# datos_pelacus = FUNCIONES_LECTURA.lectura_datos_pelacus(archivo_datos)  
 
# # Realiza un control de calidad primario a los datos importados   
# print('Realizando control de calidad')
# datos_pelacus_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_pelacus,direccion_host,base_datos,usuario,contrasena,puerto)  

# # Recupera el identificador del programa de muestreo
# id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra la estación asociada a cada registro
# print('Identificando la estación correspondiente a cada medida')
# datos_pelacus_corregido = FUNCIONES_PROCESADO.evalua_estaciones(datos_pelacus_corregido,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# # Asigna el identificador de salida al mar correspondiente
# tipo_salida = 'ANUAL'
# datos_pelacus_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos_pelacus_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra el identificador asociado a cada registro
# print('Identificando el registro correspondiente a cada medida')
# datos_pelacus_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_pelacus_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# print('Insertando los datos en la base de datos')
# FUNCIONES_PROCESADO.inserta_datos(datos_pelacus_corregido,'fisica',direccion_host,base_datos,usuario,contrasena,puerto)
# FUNCIONES_PROCESADO.inserta_datos(datos_pelacus_corregido,'bgq',direccion_host,base_datos,usuario,contrasena,puerto)
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





