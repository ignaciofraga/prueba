# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""







import FUNCIONES_PROCESADO
import FUNCIONES_LECTURA
import pandas
pandas.options.mode.chained_assignment = None
import datetime
import os
import pandas.io.sql as psql
from sqlalchemy import create_engine
import numpy
import psycopg2
from glob import glob





def inserta_radiales_historico(ruta_archivos,anho,nombre_programa,base_datos,usuario,contrasena,puerto,direccion_host):

    # Recupera el identificador del programa
    id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto)
                
    # recupera la información de las estaciones incluidas en la base de datos
    con_engine          = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql           = create_engine(con_engine)
    tabla_estaciones    = psql.read_sql('SELECT * FROM estaciones', conn_psql)
    tabla_salidas       = psql.read_sql('SELECT * FROM salidas_muestreos', conn_psql)
    conn_psql.dispose()
    
    tabla_estaciones_programa = tabla_estaciones[tabla_estaciones['programa']==int(id_programa)]
    tabla_salidas_programa    = tabla_salidas[tabla_salidas['programa']==int(id_programa)]
    
    ruta_datos = ruta_archivos + '/' + str(anho) + '/*/'
    
    # Recupera el nombre de los directorios
    listado_salidas = glob(ruta_datos, recursive = True)
    
    # Mantén sólo la parte de fechas
    for isalida in range(len(listado_salidas)):
    #for isalida in range(1):    
        
        print('Procesando botelleros de la salida ',listado_salidas[isalida])
        
        #### DATOS DE BOTELLEROS    
        
        try:
            ruta_botelleros = listado_salidas[isalida] + 'btl+PAR+flscufa+O2'
            os.chdir(ruta_botelleros)
        except:
            ruta_botelleros = listado_salidas[isalida] + 'btl+PAR+flscufa'        
            os.chdir(ruta_botelleros)
                
        for archivo in glob("*.btl"):

            # Encuentra el identificador de la estación
            posicion_inicio    = archivo.find('e') + 1
            posicion_final     = archivo.find('.')
            nombre_estacion    = archivo[posicion_inicio:posicion_final].upper() #+ 'CO' 
            id_estacion        = tabla_estaciones_programa['id_estacion'][tabla_estaciones_programa['nombre_estacion']==str(nombre_estacion)].iloc[0]
        
            # Encuentra el identificador de la salida
            posicion_final     = archivo.find('e') 
            fecha_salida       = datetime.datetime.strptime(archivo[0:posicion_final], '%Y%m%d').date()                     
            
            id_salida          = tabla_salidas_programa['id_salida'][tabla_salidas_programa['fecha_salida']==fecha_salida].iloc[0]
            
            # Lee los archivos .btl
            nombre_archivo = archivo
            lectura_archivo = open(archivo, "r")  
            datos_archivo = lectura_archivo.readlines()
                  
            mensaje_error,datos_botellas,io_par,io_fluor,io_O2 = FUNCIONES_LECTURA.lectura_btl(nombre_archivo,datos_archivo)
            
            datos_botellas = FUNCIONES_PROCESADO.procesado_botella(datos_botellas,id_estacion,nombre_estacion,id_programa,id_salida,tabla_estaciones_programa)
   
            # Aplica control de calidad
            datos_botellas,textos_aviso        = FUNCIONES_PROCESADO.control_calidad(datos_botellas)            
   
            # Asigna el registro correspondiente a cada muestreo e introduce la información en la base de datos
            datos_botellas = FUNCIONES_PROCESADO.evalua_registros(datos_botellas,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
 
            FUNCIONES_PROCESADO.inserta_datos(datos_botellas,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)    
   
               
        ### DATOS DE PERFILES

        print('Procesando perfiles de la salida ',listado_salidas[isalida])
           
        os.chdir(listado_salidas[isalida])
        for archivo in glob("*.cnv"):
            
            
            posicion_inicio    = archivo.find('e') + 1
            posicion_final     = archivo.find('.')
            nombre_estacion    = archivo[posicion_inicio:posicion_final].upper() #+ 'CO' 
            
            id_estacion = tabla_estaciones_programa['id_estacion'][tabla_estaciones_programa['nombre_estacion']==str(nombre_estacion)].iloc[0]
                           
            # Lectura de la información contenida en el archivo como un dataframe
            lectura_archivo = open(archivo, "r")  
            datos_archivo = lectura_archivo.readlines()
                       
            datos_perfil,df_perfiles,datos_muestreo_perfil = FUNCIONES_LECTURA.lectura_archivo_perfiles(datos_archivo)
            
            FUNCIONES_PROCESADO.procesado_perfiles(datos_perfil,datos_muestreo_perfil,df_perfiles,id_salida,id_programa,abreviatura_programa,nombre_estacion,id_estacion,direccion_host,base_datos,usuario,contrasena,puerto)
                                    

       

 
       
