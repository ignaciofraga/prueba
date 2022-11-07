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

        #if fecha_salida.day <10:
        #    nombre_salida = nombre_programa + ' ' + tipo_salida + ' ' + meses[fecha_salida.month-2] + ' ' + str(fecha_salida.year) 
        #else:
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
    
    print(id_salida,nombre_salida,json_estaciones)
    
    # conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    # cursor = conn.cursor()   

    # for archivo in os.listdir(ruta_salida):
    #     if archivo.endswith(".btl"):

    #         print(ruta_salida)   
    #         print(archivo)    

    #         nombre_archivo = archivo
    #         lectura_archivo = open(archivo, "r")  
    #         datos_archivo = lectura_archivo.readlines()
            
    #         mensaje_error,datos_botellas,io_par,io_fluor,io_O2 = FUNCIONES_INSERCION.lectura_btl(nombre_archivo,datos_archivo,nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto)

    #         # Asigna el identificador de la salida al mar
    #         datos_botellas = FUNCIONES_INSERCION.evalua_salidas(datos_botellas,id_programa,nombre_programa,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
            
    #         # Asigna el registro correspondiente a cada muestreo e introduce la información en la base de datos
    #         datos_botellas = FUNCIONES_INSERCION.evalua_registros(datos_botellas,nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto)


    #         # Inserta datos físicos
    #         for idato in range(datos_botellas.shape[0]):
    #             if io_par == 1:
    #                 instruccion_sql = '''INSERT INTO datos_discretos_fisica (id_disc_fisica,muestreo,temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf)
    #                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf) = ROW (EXCLUDED.temperatura_ctd,EXCLUDED.temperatura_ctd_qf,EXCLUDED.salinidad_ctd,EXCLUDED.salinidad_ctd_qf,EXCLUDED.par_ctd,EXCLUDED.par_ctd_qf);''' 
                    
               
    #                 cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['temperatura_ctd'][idato],int(datos_botellas['temperatura_ctd_qf'][idato]),datos_botellas['salinidad_ctd'][idato],int(datos_botellas['salinidad_ctd_qf'][idato]),datos_botellas['par_ctd'][idato],int(datos_botellas['par_ctd_qf'][idato])))
    #                 conn.commit()
                    
    #             if io_par == 0:   
    #                 instruccion_sql = '''INSERT INTO datos_discretos_fisica (id_disc_fisica,muestreo,temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf)
    #                       VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf) = ROW (EXCLUDED.temperatura_ctd,EXCLUDED.temperatura_ctd_qf,EXCLUDED.salinidad_ctd,EXCLUDED.salinidad_ctd_qf);''' 
                            
    #                 cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['temperatura_ctd'][idato],int(datos_botellas['temperatura_ctd_qf'][idato]),datos_botellas['salinidad_ctd'][idato],int(datos_botellas['salinidad_ctd_qf'][idato])))
    #                 conn.commit()
                    
    #             # Inserta datos biogeoquímicos
    #             if io_fluor == 1:                
    #                 instruccion_sql = '''INSERT INTO datos_discretos_biogeoquimica (id_disc_biogeoquim,muestreo,fluorescencia_ctd,fluorescencia_ctd_qf)
    #                       VALUES (%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (fluorescencia_ctd,fluorescencia_ctd_qf) = ROW (EXCLUDED.fluorescencia_ctd,EXCLUDED.fluorescencia_ctd_qf);''' 
                            
    #                 cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['fluorescencia_ctd'][idato],int(datos_botellas['fluorescencia_ctd'][idato])))
    #                 conn.commit()           
     
    #             if io_O2 == 1:                
    #                 instruccion_sql = '''INSERT INTO datos_discretos_biogeoquimica (id_disc_biogeoquim,muestreo,oxigeno_ctd,oxigeno_ctd_qf)
    #                       VALUES (%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (oxigeno_ctd,oxigeno_ctd_qf) = ROW (EXCLUDED.oxigeno_ctd,EXCLUDED.oxigeno_ctd_qf);''' 
                            
    #                 cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['oxigeno_ctd'][idato],int(datos_botellas['oxigeno_ctd_qf'][idato])))
    #                 conn.commit()   

              
                  
    # cursor.close()
    # conn.close()           
    
    
    
