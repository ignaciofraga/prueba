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
        
        df_acc = pandas.DataFrame()
        
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
                  
            mensaje_error,datos_botellas,io_par,io_fluor,io_O2 = FUNCIONES_LECTURA.lectura_btl(nombre_archivo,datos_archivo,nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto)
        
            for imuestreo in range(datos_botellas.shape[0]):
                if datos_botellas['latitud'].iloc[imuestreo] is None:
                    datos_botellas['latitud'].iloc[imuestreo] = tabla_estaciones_programa['latitud_estacion'][tabla_estaciones_programa['id_estacion']==id_estacion].iloc[0]
                if datos_botellas['longitud'].iloc[imuestreo] is None:
                    datos_botellas['longitud'].iloc[imuestreo] = tabla_estaciones_programa['longitud_estacion'][tabla_estaciones_programa['id_estacion']==id_estacion].iloc[0]
        
            # Aplica control de calidad
            #datos_botellas,textos_aviso                = FUNCIONES_PROCESADO.control_calidad(datos_botellas,direccion_host,base_datos,usuario,contrasena,puerto)            
            datos_botellas['id_estacion_temp']         = datos_botellas['estacion']

            datos_botellas['fecha_muestreo']    = fecha_salida

            df_acc = pandas.concat([df_acc, datos_botellas])


        
        # Asigna el identificador de la salida al mar
        df_acc['id_salida'] =  id_salida
        
        # Define un nuevo índice de filas. Si se han eliminado registros este paso es necesario
        indices_dataframe        = numpy.arange(0,df_acc.shape[0],1,dtype=int)    
        df_acc['id_temp'] = indices_dataframe
        df_acc.set_index('id_temp',drop=False,append=False,inplace=True)
    
    
        datos_botellas,textos_aviso = FUNCIONES_PROCESADO.control_calidad(df_acc,direccion_host,base_datos,usuario,contrasena,puerto)  
    
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
            
            # Define el nombre del perfil
            nombre_perfil = abreviatura_programa + '_' + (datos_muestreo_perfil['fecha_muestreo'].iloc[0]).strftime("%Y%m%d") + '_E' + str(nombre_estacion) + '_C' + str(datos_muestreo_perfil['cast_muestreo'].iloc[0])

            # Recupera el identificador del perfil
            datos_muestreo_perfil['id_estacion'] = id_estacion
            datos_muestreo_perfil['id_salida']   = id_salida
            datos_muestreo_perfil = FUNCIONES_PROCESADO.evalua_perfiles(nombre_perfil,datos_muestreo_perfil,direccion_host,base_datos,usuario,contrasena,puerto)

            # Asigna el identificador al perfil
            df_perfiles['perfil'] = datos_muestreo_perfil['perfil'].iloc[0]

            #Inserta en la base de datos
            FUNCIONES_PROCESADO.inserta_datos(df_perfiles,'perfil',direccion_host,base_datos,usuario,contrasena,puerto)       
                
            
            if nombre_estacion == '2':  # Estacion 2 del programa radiales, añadir muestreo correspondiente a la botella en superficie
    
                  # Genera dataframe con el muestreo de la estacion 2
                  pres_min                             = min(datos_perfil['presion_ctd'])
                  df_temp                              = datos_perfil[datos_perfil['presion_ctd']==pres_min]
                 
                  # Elimina la fila correspondiente al comienzo del descenso
                  if df_temp.shape[0] > 1:
                      df_botella = df_temp.drop([0])
                  else:
                      df_botella = df_temp
    
    
                  # Asigna los datos correspondientes
                  df_botella['latitud']                = datos_muestreo_perfil['lat_muestreo'].iloc[0]
                  df_botella['longitud']               = datos_muestreo_perfil['lon_muestreo'].iloc[0]
                  df_botella['prof_referencia']        = 0
                  df_botella['fecha_muestreo']         = datos_muestreo_perfil['fecha_muestreo'].iloc[0]
                  df_botella = df_botella.drop(columns = ['c0S/m','flag'])
                  try:
                      df_botella = df_botella.drop(columns = ['sbeox0V','sbeox0ML/L'])
                      df_botella['oxigeno_ctd_qf']   = 1
                  except:
                      pass
                  id_estacion                          = tabla_estaciones_programa['id_estacion'][tabla_estaciones_programa['nombre_estacion']==str(nombre_estacion)].iloc[0]
                  df_botella['id_estacion_temp']       = int(id_estacion) 
                  df_botella['id_salida']              = id_salida 
                  df_botella['nombre_muestreo']        = abreviatura_programa + '_' + (datos_muestreo_perfil['fecha_muestreo'].iloc[0]).strftime("%Y%m%d") + '_E2_P0' 
                 
                  df_botella['programa']               = id_programa    
                  df_botella['num_cast']               = datos_muestreo_perfil['cast_muestreo'].iloc[0] 
                  # Añade botella y hora de muestreo (nulas) para evitar errores en el procesado
                  df_botella['botella']                = 7
                  df_botella['hora_muestreo']          = None
                  
                  # Añade qf 
                  df_botella['temperatura_ctd_qf']     = 1
                  df_botella['salinidad_ctd_qf']       = 1
                  df_botella['fluorescencia_ctd_qf']   = 1
                  df_botella['par_ctd_qf']             = 1
          
                  df_botella                           = FUNCIONES_PROCESADO.evalua_registros(df_botella,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
        
                  FUNCIONES_PROCESADO.inserta_datos(df_botella,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)              
                
       

 
       
