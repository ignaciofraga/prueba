# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""







import FUNCIONES_INSERCION
import pandas
pandas.options.mode.chained_assignment = None
import datetime

import os
import numpy
import pandas.io.sql as psql
from sqlalchemy import create_engine
import psycopg2
import os
from glob import glob
import json

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
nombre_programa = 'RADIAL CORUÑA'

anho = 2022
ruta_archivos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/MENSUALES/Procesados'
tipo_salida   = 'MENSUAL' 

# Recupera el identificador del programa
id_programa = FUNCIONES_INSERCION.recupera_id_programa(nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto)
            
# recupera la información de las estaciones incluidas en la base de datos
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
conn_psql.dispose()

ruta_datos = ruta_archivos + '/' + str(anho) + '/*/'

# Recupera el nombre de los directorios
listado_salidas = glob(ruta_datos, recursive = True)

# Mantén sólo la parte de fechas
for idato in range(len(listado_salidas)):

    fecha_salida_texto = listado_salidas[idato][-14:-6]

    fecha_salida       = datetime.datetime.strptime(fecha_salida_texto, '%y_%m_%d').date()
    
    # Encuentra los parámetros muestreados
    parametros = 'btl+PAR'
    
    for archivo in os.listdir(listado_salidas[idato]):
        if 'flscufa' in archivo:
            parametros = parametros + '+flscufa'

        if 'O2' in archivo:
            parametros = parametros + '+O2'
            
            
    
    # Encuentra las estaciones muestreadas a partir de los archivos .btl
    ruta_salida   = listado_salidas[idato] + '/' + parametros + '/'
    
    listado_estaciones_muestreadas = []
    for archivo in os.listdir(ruta_salida):
        if archivo.endswith(".btl"):
            posicion_separador = archivo.index('+')
            nombre_estacion    = archivo[8:posicion_separador].upper() + 'CO'
            
            listado_estaciones_muestreadas.append(nombre_estacion)
    
    # Añade una salida al mar con esas estaciones
            
    json_estaciones        = json.dumps(listado_estaciones_muestreadas)

    instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
    VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO UPDATE SET (nombre_salida,nombre_programa,tipo_salida,fecha_retorno,estaciones) = ROW(EXCLUDED.nombre_salida,EXCLUDED.nombre_programa,EXCLUDED.tipo_salida,EXCLUDED.fecha_retorno,EXCLUDED.estaciones);''' 
        
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()

    if tipo_salida == 'MENSUAL':
        # Asigna nombre a la salida
        meses = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']

        nombre_salida = nombre_programa + ' ' + tipo_salida + ' ' + meses[fecha_salida.month-1] + ' ' + str(fecha_salida.year) 
  
    cursor.execute(instruccion_sql, (nombre_salida,int(id_programa),nombre_programa,tipo_salida,fecha_salida,fecha_salida,json_estaciones))
    conn.commit()
    
    # Recupera el identificador de la salida al mar
    instruccion_sql = "SELECT id_salida FROM salidas_muestreos WHERE nombre_salida = '" + nombre_salida + "';"
    cursor.execute(instruccion_sql)
    id_salida =cursor.fetchone()[0]
    conn.commit()
    
    cursor.close()
    conn.close()
    
    
    os.chdir(ruta_salida)
    


# instruccion_sql = '''INSERT INTO datos_discretos_fisica (id_centro,nombre_centro)
#     VALUES (%s,%s) ON CONFLICT (id_centro) DO UPDATE SET (nombre_centro) = ROW(EXCLUDED.nombre_centro);''' 
        
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()
# for idato in range(len(centros)):
#     cursor.execute(instruccion_sql, (identificador[idato],centros[idato]))
#     conn.commit()
# cursor.close()
# conn.close()

    
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()   

    for archivo in os.listdir(ruta_salida):
        if archivo.endswith(".btl"):

            print(ruta_salida)   
            print(archivo)    

            nombre_archivo = archivo
            lectura_archivo = open(archivo, "r")  
            datos_archivo = lectura_archivo.readlines()
            
            mensaje_error,datos_botellas,io_par,io_fluor,io_O2 = FUNCIONES_INSERCION.lectura_btl(nombre_archivo,datos_archivo,nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto)

            # Asigna el identificador de la salida al mar
            datos_botellas = FUNCIONES_INSERCION.evalua_salidas(datos_botellas,id_programa,nombre_programa,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
            
            # Asigna el registro correspondiente a cada muestreo e introduce la información en la base de datos
            datos_botellas = FUNCIONES_INSERCION.evalua_registros(datos_botellas,nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto)


            # Inserta datos físicos
            for idato in range(datos_botellas.shape[0]):
                if io_par == 1:
                    instruccion_sql = '''INSERT INTO datos_discretos_fisica (id_disc_fisica,muestreo,temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf) = ROW (EXCLUDED.temperatura_ctd,EXCLUDED.temperatura_ctd_qf,EXCLUDED.salinidad_ctd,EXCLUDED.salinidad_ctd_qf,EXCLUDED.par_ctd,EXCLUDED.par_ctd_qf);''' 
                    
               
                    cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['temperatura_ctd'][idato],int(datos_botellas['temperatura_ctd_qf'][idato]),datos_botellas['salinidad_ctd'][idato],int(datos_botellas['salinidad_ctd_qf'][idato]),datos_botellas['par_ctd'][idato],int(datos_botellas['par_ctd_qf'][idato])))
                    conn.commit()
                    
                if io_par == 0:   
                    instruccion_sql = '''INSERT INTO datos_discretos_fisica (id_disc_fisica,muestreo,temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf)
                         VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf) = ROW (EXCLUDED.temperatura_ctd,EXCLUDED.temperatura_ctd_qf,EXCLUDED.salinidad_ctd,EXCLUDED.salinidad_ctd_qf);''' 
                            
                    cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['temperatura_ctd'][idato],int(datos_botellas['temperatura_ctd_qf'][idato]),datos_botellas['salinidad_ctd'][idato],int(datos_botellas['salinidad_ctd_qf'][idato])))
                    conn.commit()
                    
                # Inserta datos biogeoquímicos
                if io_fluor == 1:                
                    instruccion_sql = '''INSERT INTO datos_discretos_biogeoquimica (id_disc_biogeoquim,muestreo,fluorescencia_ctd,fluorescencia_ctd_qf)
                         VALUES (%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (fluorescencia_ctd,fluorescencia_ctd_qf) = ROW (EXCLUDED.fluorescencia_ctd,EXCLUDED.fluorescencia_ctd_qf);''' 
                            
                    cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['fluorescencia_ctd'][idato],int(datos_botellas['fluorescencia_ctd'][idato])))
                    conn.commit()           
     
                if io_O2 == 1:                
                    instruccion_sql = '''INSERT INTO datos_discretos_biogeoquimica (id_disc_biogeoquim,muestreo,oxigeno_ctd,oxigeno_ctd_qf)
                         VALUES (%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (oxigeno_ctd,oxigeno_ctd_qf) = ROW (EXCLUDED.oxigeno_ctd,EXCLUDED.oxigeno_ctd_qf);''' 
                            
                    cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['oxigeno_ctd'][idato],int(datos_botellas['oxigeno_ctd_qf'][idato])))
                    conn.commit()   


            # for idato in range(datos_botellas.shape[0]):
            #     if io_par == 1:
            #         instruccion_sql = '''INSERT INTO datos_discretos_fisica (muestreo,temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf)
            #              VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf) = ROW (EXCLUDED.temperatura_ctd,EXCLUDED.temperatura_ctd_qf,EXCLUDED.salinidad_ctd,EXCLUDED.salinidad_ctd_qf,EXCLUDED.par_ctd,EXCLUDED.par_ctd_qf);''' 
                    
               
            #         cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['temperatura_ctd'][idato],int(datos_botellas['temperatura_ctd_qf'][idato]),datos_botellas['salinidad_ctd'][idato],int(datos_botellas['salinidad_ctd_qf'][idato]),datos_botellas['par_ctd'][idato],int(datos_botellas['par_ctd_qf'][idato])))
            #         conn.commit()
                    
            #     if io_par == 0:   
            #         instruccion_sql = '''INSERT INTO datos_discretos_fisica (muestreo,temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf)
            #              VALUES (%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf) = ROW(EXCLUDED.temperatura_ctd,EXCLUDED.temperatura_ctd_qf,EXCLUDED.salinidad_ctd,EXCLUDED.salinidad_ctd_qf);''' 
                            
            #         cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['temperatura_ctd'][idato],int(datos_botellas['temperatura_ctd_qf'][idato]),datos_botellas['salinidad_ctd'][idato],int(datos_botellas['salinidad_ctd_qf'][idato])))
            #         conn.commit()
                    
            #     # Inserta datos biogeoquímicos
            #     if io_fluor == 1:                
            #         instruccion_sql = '''INSERT INTO datos_discretos_biogeoquimica (muestreo,fluorescencia_ctd,fluorescencia_ctd_qf)
            #              VALUES (%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (fluorescencia_ctd,fluorescencia_ctd_qf) = ROW(EXCLUDED.fluorescencia_ctd,EXCLUDED.fluorescencia_ctd_qf);''' 
                            
            #         cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['fluorescencia_ctd'][idato],int(datos_botellas['fluorescencia_ctd'][idato])))
            #         conn.commit()           
     
            #     if io_O2 == 1:                
            #         instruccion_sql = '''INSERT INTO datos_discretos_biogeoquimica (muestreo,oxigeno_ctd,oxigeno_ctd_qf)
            #              VALUES (%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (oxigeno_ctd,oxigeno_ctd_qf) = ROW(EXCLUDED.oxigeno_ctd,EXCLUDED.oxigeno_ctd_qf);''' 
                            
            #         cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['oxigeno_ctd'][idato],int(datos_botellas['oxigeno_ctd_qf'][idato])))
            #         conn.commit()                 
                  
    cursor.close()
    conn.close()           
    
    
    
                
#             io_par         = 0
#             io_fluor       = 0
#             io_O2          = 0


# ' muestreo int NOT NULL,'
# ' temperatura_ctd NUMERIC (4, 2),'
# ' temperatura_ctd_qf int DEFAULT 9,'
# ' salinidad_ctd NUMERIC (5, 3),'
# ' salinidad_ctd_qf int DEFAULT 9,'
# ' par_ctd NUMERIC (8, 3),'
# ' par_ctd_qf int DEFAULT 9,'
# ' turbidez_ctd NUMERIC (6, 3),'
# ' turbidez_ctd_qf int DEFAULT 9,'

            # # Inserta en la base de datos las variables físicas disponibles 
            # FUNCIONES_INSERCION.inserta_datos_fisica(datos_botellas,direccion_host,base_datos,usuario,contrasena,puerto)
            
            # # Inserta en la base de datos las variables biogeoquímicas disponibles 
            # FUNCIONES_INSERCION.inserta_datos_biogeoquimica(datos_botellas,direccion_host,base_datos,usuario,contrasena,puerto)
            
            
    #print(fecha_salida,listado_estaciones_muestreadas)            

            #print(nombre_estacion,id_estacion)











# estacion_muestreo = '2'

# archivo_btl       = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/GENERALES/20221004e2.btl'

# archivo_temporal  = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/GENERALES/20221004e2_temp.btl'

# fecha_muestreo    = datetime.date(2022,10,4)

# # Define las profundidades de referencia en cada estación
# if estacion_muestreo == '2': #Estación 2
#     profundidades_referencia = numpy.asarray([0,5,10,20,30,40,70])
# if estacion_muestreo == '4': #Estación 2        
#     profundidades_referencia = numpy.asarray([0,4,8,12,18]) 
# if estacion_muestreo == '3c': #Estación 2        
#      profundidades_referencia = numpy.asarray([0,5,10,20,30,35,40]) 

# # Encuentra el identificador del programa seleccionado
# con_engine          = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn_psql           = create_engine(con_engine)
# df_programas        = psql.read_sql('SELECT * FROM programas', conn_psql)
# id_programa_elegido = int(df_programas['id_programa'][df_programas['nombre_programa']==programa_muestreo].values[0])

# # Encuentra el identificador correspondiente a la estación seleccionada
# df_estaciones          = psql.read_sql('SELECT * FROM estaciones', conn_psql)
# df_estaciones_programa =  df_estaciones[df_estaciones['programa']==id_programa_elegido]
# id_estacion_elegida    = int(df_estaciones_programa['id_estacion'][df_estaciones_programa['nombre_estacion']==estacion_muestreo].values[0])

# # Recupera los datos disponibles
# df_datos_fisicos        = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn_psql)
# df_datos_biogeoquimicos = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn_psql)

# conn_psql.dispose() # Cierra la conexión con la base de datos 

# # Lee el archivo .btl y escribe la información de las botellas en un archivo temporal
# cast_muestreo = 1 # Asinga este valor por si no se introdujo ningún dato en el muestreo
# with open(archivo_btl, "r") as file_input:
#     with open(archivo_temporal, "w") as output:
#         for line in file_input:
#             if line[0:8] == '** Time:': # Línea con hora del cast
#                 hora_muestreo = datetime.datetime.strptime(line[8:-1],'%H:%M').time()            
#             if line[0:8] == '** Cast:': # Línea con el número de cast
#                 cast_muestreo = int(line[8:-1])
#             if line[-6:-1] == '(avg)': # Línea con datos de botella, escribir en el archivo temporal
#                 output.write(line)

# file_input.close()
# output.close()

# # Lee el archivo temporal como  un dataframe
# datos_botellas = pandas.read_csv(archivo_temporal, sep='\s+',header=None)

# # Elimina las columnas que no interesan
# datos_botellas = datos_botellas.drop(columns=[1,2,3,7,8,11,13,14])

# # Cambia el nombre de las columnas
# datos_botellas = datos_botellas.rename(columns={0: 'botella', 4: 'salinidad_ctd',5:'presion_ctd',6:'temperatura_ctd',9:'par_ctd',10:'fluorescencia_ctd',12:'oxigeno_ctd'})

# # Elimina el archivo temporal
# os.remove(archivo_temporal)

# # Añade una columna con la profundidad de referencia
# datos_botellas['prof_referencia'] = numpy.zeros(datos_botellas.shape[0],dtype=int)
# for idato in range(datos_botellas.shape[0]):
#         # Encuentra la profundidad de referencia más cercana a cada dato
#         idx = (numpy.abs(profundidades_referencia - datos_botellas['presion_ctd'][idato])).argmin()
#         datos_botellas['prof_referencia'][idato] =  profundidades_referencia[idx]

# # Redondea la precisión de los datos de profundidad
# datos_botellas['presion_ctd'] = round(datos_botellas['presion_ctd'],2)
    
# # Añade columnas con datos del muestreo 
# datos_botellas['id_estacion_temp']         = numpy.zeros(datos_botellas.shape[0],dtype=int)
# datos_botellas['id_estacion_temp']         = id_estacion_elegida
# datos_botellas['fecha_muestreo']           = fecha_muestreo
# datos_botellas['hora_muestreo']            = hora_muestreo
# datos_botellas['num_cast']                 = cast_muestreo
# datos_botellas['configuracion_perfilador'] = 1
# datos_botellas['configuracion_superficie'] = 1


# # Asigna el registro correspondiente a cada muestreo e introduce la información en la base de datos
# datos_botellas = FUNCIONES_INSERCION.evalua_registros(datos_botellas,programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)


# #Inserta datos física (temperatura, salinidad y PAR)
# instruccion_sql = '''INSERT INTO datos_discretos_fisica (id_disc_fisica,muestreo,temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf)
#       VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf) = ROW(EXCLUDED.temperatura_ctd,EXCLUDED.temperatura_ctd_qf,EXCLUDED.salinidad_ctd,EXCLUDED.salinidad_ctd_qf,EXCLUDED.par_ctd,EXCLUDED.par_ctd_qf);''' 
        
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()
# for idato in range(datos_botellas.shape[0]):
#     cursor.execute(instruccion_sql, (int(df_datos_fisicos.shape[0]+idato+1),int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['temperatura_ctd'][idato],int(2),datos_botellas['salinidad_ctd'][idato],int(2),datos_botellas['par_ctd'][idato],int(2)))
#     conn.commit()
# cursor.close()
# conn.close()

# #Inserta datos biogeoquímica (oxígeno y fluorescencia)
# instruccion_sql = '''INSERT INTO datos_discretos_biogeoquimica (id_disc_biogeoquim,muestreo,fluorescencia_ctd,fluorescencia_ctd_qf,oxigeno_ctd,oxigeno_ctd_qf)
#       VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (fluorescencia_ctd,fluorescencia_ctd_qf,oxigeno_ctd,oxigeno_ctd_qf) = (EXCLUDED.fluorescencia_ctd,EXCLUDED.fluorescencia_ctd_qf,EXCLUDED.oxigeno_ctd,EXCLUDED.oxigeno_ctd_qf);''' 
        
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()
# for idato in range(datos_botellas.shape[0]):
#     cursor.execute(instruccion_sql, (int(df_datos_biogeoquimicos.shape[0]+idato+1),int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['fluorescencia_ctd'][idato],int(2),datos_botellas['oxigeno_ctd'][idato],int(2)))
#     conn.commit()
# cursor.close()
# conn.close()




# #Inserta datos física
# instruccion_sql = '''INSERT INTO datos_discretos_fisica (muestreo,temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf,turbidez_ctd_qf)
#       VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO NOTHING;''' 
        
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()
# for idato in range(datos_botellas.shape[0]):
#     cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['temperatura_ctd'][idato],int(2),datos_botellas['salinidad_ctd'][idato],int(2),datos_botellas['par_ctd'][idato],int(2),int(9)))
#     conn.commit()
# cursor.close()
# conn.close()

# instruccion_sql = '''INSERT INTO datos_discretos_fisica (muestreo,temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf,turbidez_ctd_qf)
#      VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (temperatura_ctd) VALUES (%s);''' 
        
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()
# for idato in range(datos_botellas.shape[0]):
#     cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['temperatura_ctd'][idato],int(2),datos_botellas['salinidad_ctd'][idato],int(2),datos_botellas['par_ctd'][idato],int(2),int(9),datos_botellas['temperatura_ctd'][idato],int(2)))
#     conn.commit()
# cursor.close()
# conn.close()

# # Inserta en la base de datos las variables físicas disponibles 
# FUNCIONES_INSERCION.inserta_datos_fisica(datos_botellas_corregidos,direccion_host,base_datos,usuario,contrasena,puerto)

# # Inserta en la base de datos las variables biogeoquímicas disponibles 
# FUNCIONES_INSERCION.inserta_datos_biogeoquimica(datos_botellas_corregidos,direccion_host,base_datos,usuario,contrasena,puerto)

