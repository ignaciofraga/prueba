# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""







import FUNCIONES_LECTURA
import FUNCIONES_PROCESADO
import pandas
pandas.options.mode.chained_assignment = None
from sqlalchemy import create_engine
import pandas.io.sql as psql
from os import listdir
from os.path import isfile, join
from glob import glob
import os
import datetime
import psycopg2



# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'
programa_muestreo = 'RADIAL CORUÑA'

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# Carga informacion común a todas las salidas
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn             = create_engine(con_engine)
tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
tabla_variables  = psql.read_sql('SELECT * FROM variables_procesado', conn)
tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
conn.dispose() 



# # Encuentra las salidas del programa
# listado_salidas = tabla_salidas['id_salida'][tabla_salidas['programa']==id_programa]

# for isalida in range(len(listado_salidas)):

#     # Inserta en la base de datos
#     conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
#     cursor = conn.cursor()                      
#     instruccion_sql = 'DELETE FROM muestreos_discretos WHERE salida_mar = ' + str(listado_salidas.iloc[isalida]) + ';'        
#     cursor.execute(instruccion_sql)
#     conn.commit()
#     cursor.close()
#     conn.close()



# # DATOS HISTORICOS (1988 - 2012)

# # Carga de informacion disponible
# con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn             = create_engine(con_engine)
# tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
# tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
# tabla_datos      = psql.read_sql('SELECT * FROM datos_discretos', conn)
# conn.dispose() 

# print('Lectura de excel con los datos históricos') 
# nombre_archivo    = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/HISTORICO/HISTORICO_MODIFICADO.xlsx'
# datos_radiales_historicos = FUNCIONES_LECTURA.lectura_radiales_historicos(nombre_archivo)

# print('Realizando control de calidad')
# datos_radiales_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_radiales_historicos)  

# # Encuentra la estación asociada a cada registro
# print('Asignando la estación correspondiente a cada medida')
# datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_estaciones(datos_radiales_corregido,id_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_muestreos)

# # Encuentra las salidas al mar correspondientes
# tipo_salida = 'MENSUAL'   
# datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos_radiales_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_salidas,tabla_muestreos)
 
# # Encuentra el identificador asociado a cada registro
# print('Asignando el registro correspondiente a cada medida')
# datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_radiales_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)
   
# # # # # # Introduce los datos en la base de datos
# print('Introduciendo los datos en la base de datos')
# texto_insercion = FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos,tabla_muestreos)







# # # RADIALES 2013-2020
# Listado de archivos disponibles
directorio_datos           = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES'
listado_archivos = [f for f in listdir(directorio_datos) if isfile(join(directorio_datos, f))]

#listado_archivos = ['RADIAL_BTL_COR_2021.xlsx']


for iarchivo in range(len(listado_archivos)):

    # Carga de informacion previa
    conn             = create_engine(con_engine)
    tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
    tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
    tabla_datos      = psql.read_sql('SELECT * FROM datos_discretos', conn)
    conn.dispose() 
    
    nombre_archivo = directorio_datos + '/' + listado_archivos[iarchivo]
    print('Procesando la informacion correspondiente al año ',nombre_archivo[-9:-5])
    
    # Lectura
    print('Leyendo los datos contenidos en el archivo excel')
    datos_radiales = FUNCIONES_LECTURA.lectura_datos_radiales(nombre_archivo,direccion_host,base_datos,usuario,contrasena,puerto)
    
    # Control de calidad
    print('Realizando control de calidad')
    datos_radiales_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_radiales)  
    
    # Encuentra la estación asociada a cada registro
    print('Asignando la estación correspondiente a cada medida')
    datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_estaciones(datos_radiales_corregido,id_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_muestreos)
    
    # Encuentra las salidas al mar correspondientes
    tipo_salida = 'MENSUAL'   
    datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos_radiales_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto,tabla_estaciones,tabla_salidas,tabla_muestreos)
     
    # Encuentra el identificador asociado a cada registro
    print('Asignando el registro correspondiente a cada medida')
    datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_radiales_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)
       
    # # # # # Introduce los datos en la base de datos
    print('Introduciendo los datos en la base de datos')
    texto_insercion = FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos,tabla_muestreos)



# # # DATOS DEL 21-23
# anhos = [2021]
# ruta_archivos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/MENSUALES/Procesados'

# # Carga informacion de las salidas y estaciones
# con_engine          = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn_psql           = create_engine(con_engine)
# tabla_estaciones    = psql.read_sql('SELECT * FROM estaciones', conn_psql)
# tabla_salidas       = psql.read_sql('SELECT * FROM salidas_muestreos', conn_psql)
# conn_psql.dispose()

# tabla_estaciones_programa = tabla_estaciones[tabla_estaciones['programa']==int(id_programa)]
# tabla_salidas_programa    = tabla_salidas[tabla_salidas['programa']==int(id_programa)]

# for ianho in range(len(anhos)):
    
#     # Lee los directorios de las salidas disponibles
#     anho = anhos[ianho]
#     ruta_datos = ruta_archivos + '/' + str(anho) + '/*/'
#     listado_salidas = glob(ruta_datos, recursive = True)
    
#     # Carga los datos de cada salida
#     for isalida in range(len(listado_salidas)): 
#     #for isalida in range(1):     
#         print('Procesando botelleros de la salida ',listado_salidas[isalida][-24:-1])
        
#         #### DATOS DE BOTELLEROS    
        
#         try:
#             ruta_botelleros = listado_salidas[isalida] + 'btl+PAR+flscufa+O2'
#             os.chdir(ruta_botelleros)
#         except:
#             ruta_botelleros = listado_salidas[isalida] + 'btl+PAR+flscufa'        
#             os.chdir(ruta_botelleros)
                
#         df_datos_salida = pandas.DataFrame()
            
#         for archivo in glob("*.btl"):
            
#             # Carga informacion
#             conn                  = create_engine(con_engine)
#             tabla_muestreos       = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
#             tabla_salidas         = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
#             tabla_datos_discretos = psql.read_sql('SELECT * FROM datos_discretos', conn)
#             conn.dispose()

#             # Encuentra el identificador de la estación
#             posicion_inicio    = archivo.find('e') + 1
#             posicion_final     = archivo.find('.')
#             nombre_estacion    = archivo[posicion_inicio:posicion_final].upper() #+ 'CO' 
#             id_estacion        = tabla_estaciones_programa['id_estacion'][tabla_estaciones_programa['nombre_estacion']==str(nombre_estacion)].iloc[0]
        
#             # Encuentra el identificador de la salida
#             posicion_final     = archivo.find('e') 
#             fecha_salida       = datetime.datetime.strptime(archivo[0:posicion_final], '%Y%m%d').date()                     
            
#             id_salida          = tabla_salidas_programa['id_salida'][tabla_salidas_programa['fecha_salida']==fecha_salida].iloc[0]
            
#             # Lee los archivos .btl
#             nombre_archivo = archivo
#             lectura_archivo = open(archivo, "r")  
#             datos_archivo = lectura_archivo.readlines()
                  
#             mensaje_error,datos_botellas,io_par,io_fluor,io_O2 = FUNCIONES_LECTURA.lectura_btl(nombre_archivo,datos_archivo)
            
#             datos_botellas = FUNCIONES_PROCESADO.procesado_botella(datos_botellas,id_estacion,nombre_estacion,id_programa,id_salida,tabla_estaciones_programa)
               
#             df_datos_salida = pandas.concat([df_datos_salida,datos_botellas])
   
#         # Control de calidad
#         datos_salida_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(df_datos_salida)  
        
#         # Encuentra el identificador asociado a cada registro
#         datos_salida_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_salida_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)
           
#         # Introduce los datos en la base de datos
#         texto_insercion = FUNCIONES_PROCESADO.inserta_datos(datos_salida_corregido,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos_discretos,tabla_muestreos)

#         print('Salida ',listado_salidas[isalida][-24:-1],' procesada correctamente')
    
   
#         print('Procesando perfiles de la salida ',listado_salidas[isalida])
           
#         os.chdir(listado_salidas[isalida])
#         for archivo in glob("*.cnv"):
            
#             # Carga informacion
#             conn                  = create_engine(con_engine)
#             tabla_datos_perfiles  = psql.read_sql('SELECT * FROM datos_perfiles', conn)
#             conn.dispose()
            
#             posicion_inicio    = archivo.find('e') + 1
#             posicion_final     = archivo.find('.')
#             nombre_estacion    = archivo[posicion_inicio:posicion_final].upper() #+ 'CO' 
            
#             id_estacion = tabla_estaciones_programa['id_estacion'][tabla_estaciones_programa['nombre_estacion']==str(nombre_estacion)].iloc[0]
                           
#             # Lectura de la información contenida en el archivo como un dataframe
#             lectura_archivo = open(archivo, "r")  
#             datos_archivo = lectura_archivo.readlines()
                       
#             datos_perfil,df_perfiles,datos_muestreo_perfil = FUNCIONES_LECTURA.lectura_archivo_perfiles(datos_archivo)
            
#             df_botellas,df_perfiles = FUNCIONES_PROCESADO.procesado_perfiles(datos_perfil,datos_muestreo_perfil,df_perfiles,id_salida,id_programa,abreviatura_programa,nombre_estacion,id_estacion,direccion_host,base_datos,usuario,contrasena,puerto)
                           
#             if df_botellas is not None:     
                
#                 conn                  = create_engine(con_engine)
#                 tabla_datos_discretos = psql.read_sql('SELECT * FROM datos_discretos', conn)
#                 conn.dispose()
                
#                 df_botellas = FUNCIONES_PROCESADO.evalua_registros(df_botellas,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto,tabla_muestreos,tabla_estaciones,tabla_variables)
                       
#                 texto_insercion = FUNCIONES_PROCESADO.inserta_datos(df_botellas,'discreto',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos_discretos,tabla_muestreos)

#             texto_insercion = FUNCIONES_PROCESADO.inserta_datos(df_perfiles,'perfil',direccion_host,base_datos,usuario,contrasena,puerto,tabla_variables,tabla_datos_perfiles,tabla_muestreos)
         
#             conn.dispose()


  