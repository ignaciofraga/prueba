# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""







import FUNCIONES_LECTURA
import FUNCIONES_PROCESADO
import INSERTA_DATOS_RADIALES_HISTORICO
import pandas
pandas.options.mode.chained_assignment = None
import datetime
from glob import glob
from sqlalchemy import create_engine
import pandas.io.sql as psql
import psycopg2
import os
import numpy

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'
programa_muestreo = 'RADIAL CORUÑA'

# recupera la información de las estaciones incluidas en la base de datos
con_engine          = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql           = create_engine(con_engine)
tabla_estaciones    = psql.read_sql('SELECT * FROM estaciones', conn_psql)
tabla_salidas       = psql.read_sql('SELECT * FROM salidas_muestreos', conn_psql)
conn_psql.dispose()

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)
tabla_estaciones_programa = tabla_estaciones[tabla_estaciones['programa']==int(id_programa)]

# DATOS DEL 21-23
# # Parámetros
anhos = [2021,2022,2023]
anhos = [2017]
ruta_archivos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/SEMANALES/Procesados'

for ianho in range(len(anhos)):
    
    anho = anhos[ianho]

    ruta_datos = ruta_archivos + '/' + str(anho) + '/*/'
    
    # Recupera el nombre de los directorios
    listado_salidas = glob(ruta_datos, recursive = True)
    
    # Mantén sólo la parte de fechas
    for isalida in range(len(listado_salidas)):
    #for isalida in range(1):    
        
        nombre_procesado = listado_salidas[isalida][-24:-1]
        print('Procesando archivos de la salida ',nombre_procesado)
        
        #### DATOS DE PERFILES    
        
        os.chdir(listado_salidas[isalida])
        df_acc = pandas.DataFrame()
        
        for archivo in glob("*.cnv"):

            # Encuentra el identificador de la estación
            posicion_inicio    = archivo.find('e') + 1
            posicion_final     = archivo.find('.')
            nombre_estacion    = archivo[posicion_inicio:posicion_final].upper() #+ 'CO' 
            id_estacion        = tabla_estaciones_programa['id_estacion'][tabla_estaciones_programa['nombre_estacion']==str(nombre_estacion)].iloc[0]
        
            # Encuentra el identificador de la salida
            posicion_final     = archivo.find('e') 
            fecha_salida       = datetime.datetime.strptime(archivo[0:posicion_final], '%Y%m%d').date()    
        
            datos = pandas.DataFrame([fecha_salida], columns=['fecha_muestreo'])
            
            datos['id_estacion_temp'] = id_estacion
    
    
            
    
            # Encuentra las salidas al mar correspondientes
            tipo_salida              = 'SEMANAL'   
            datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
            id_salida                = datos_radiales_corregido['id_salida'].iloc[0] 
            
            # Lectura de la información contenida en el archivo como un dataframe
            lectura_archivo = open(archivo, "r")  
            datos_archivo = lectura_archivo.readlines()                      
            datos_perfil,df_perfiles,listado_variables,fecha_muestreo,hora_muestreo,cast_muestreo,lat_muestreo,lon_muestreo = FUNCIONES_LECTURA.lectura_archivo_perfiles(datos_archivo)
            
            if lat_muestreo is None:
                lat_muestreo = tabla_estaciones_programa['latitud_estacion'][tabla_estaciones_programa['nombre_estacion']==str(nombre_estacion)].iloc[0]

            if lon_muestreo is None:
                lon_muestreo = tabla_estaciones_programa['longitud_estacion'][tabla_estaciones_programa['nombre_estacion']==str(nombre_estacion)].iloc[0]

            
            # Elimina las filas en las que todos los valores son NaN
            datos_perfil.dropna(axis = 0, how = 'all', inplace = True)
            
            # Define el nombre del perfil
            nombre_perfil = abreviatura_programa + '_' + fecha_muestreo.strftime("%Y%m%d") + '_E' + str(nombre_estacion) + '_C' + str(cast_muestreo)
            
            # Obtén el identificador del perfil en la base de datos
            instruccion_sql = "INSERT INTO perfiles_verticales (nombre_perfil,estacion,salida_mar,num_cast,fecha_perfil,hora_perfil,longitud_muestreo,latitud_muestreo)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_perfil,num_cast) DO UPDATE SET (nombre_perfil,salida_mar,hora_perfil,longitud_muestreo,latitud_muestreo) = ROW(EXCLUDED.nombre_perfil,EXCLUDED.salida_mar,EXCLUDED.hora_perfil,EXCLUDED.longitud_muestreo,EXCLUDED.latitud_muestreo);"      
            
            nombre_perfil = abreviatura_programa + '_' + fecha_muestreo.strftime("%Y%m%d") + '_E' + str(nombre_estacion) + '_C' + str(cast_muestreo)
            
            conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
            cursor = conn.cursor()    
            cursor.execute(instruccion_sql,(nombre_perfil,int(id_estacion),int(id_salida),int(cast_muestreo),fecha_muestreo,hora_muestreo,lon_muestreo,lat_muestreo))
            conn.commit() 
           
            instruccion_sql = "SELECT perfil FROM perfiles_verticales WHERE nombre_perfil = '" + nombre_perfil + "';" 
            cursor = conn.cursor()    
            cursor.execute(instruccion_sql)
            id_perfil =cursor.fetchone()[0]
            conn.commit()                  
            
            df_perfiles['perfil'] = id_perfil
            
            FUNCIONES_PROCESADO.inserta_datos(df_perfiles,'perfil_fisica',direccion_host,base_datos,usuario,contrasena,puerto)
            FUNCIONES_PROCESADO.inserta_datos(df_perfiles,'perfil_bgq',direccion_host,base_datos,usuario,contrasena,puerto)        
                
            
            
            
            
            listado_presiones_ctd = [min(datos_perfil['presion_ctd']),40,max(datos_perfil['presion_ctd'])]
            listado_presiones_ref = [0,40,70]            

            for ibotella in range(len(listado_presiones_ctd)):
                
                  df_temp                              = datos_perfil[datos_perfil['presion_ctd']==listado_presiones_ctd[ibotella]]
                 
                  # Elimina la fila correspondiente al comienzo del descenso
                  if df_temp.shape[0] > 1:
                      #df_botella = df_temp.drop([0])
                      df_botella=df_temp.tail(-1)
                  else:
                      df_botella = df_temp
        
        
                  # Asigna los datos correspondientes
                  df_botella['latitud']                = lat_muestreo
                  df_botella['longitud']               = lon_muestreo
                  df_botella['prof_referencia']        = listado_presiones_ref[ibotella]
                  df_botella['fecha_muestreo']         = fecha_muestreo
                  df_botella = df_botella.drop(columns = ['c0S/m','flag'])
                  try:
                      df_botella = df_botella.drop(columns = ['sbeox0V','sbeox0ML/L'])
                      df_botella['oxigeno_ctd_qf']   = 1
                  except:
                      pass
                  df_botella['id_estacion_temp']       = int(id_estacion) 
                  df_botella['id_salida']              = id_salida 
                  df_botella['nombre_muestreo']        = abreviatura_programa + '_' + fecha_muestreo.strftime("%Y%m%d") + '_E2_P' + str(listado_presiones_ref[ibotella]) 
                 
                  df_botella['programa']               = id_programa    
                  df_botella['num_cast']               = cast_muestreo 
                  # Añade botella y hora de muestreo (nulas) para evitar errores en el procesado
                  df_botella['botella']                = None
                  df_botella['hora_muestreo']          = hora_muestreo
                  
                  # Añade qf 
                  df_botella['temperatura_ctd_qf']     = 1
                  df_botella['salinidad_ctd_qf']       = 1
          
                  df_botella                           = FUNCIONES_PROCESADO.evalua_registros(df_botella,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
        
                  FUNCIONES_PROCESADO.inserta_datos(df_botella,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)
                  FUNCIONES_PROCESADO.inserta_datos(df_botella,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)               
                

    
    
    
    
    
    
    #datos_botellas = INSERTA_DATOS_PERFILES.inserta_radiales_historico(ruta_archivos,anho,programa_muestreo,base_datos,usuario,contrasena,puerto,direccion_host)       


# ####################
# # Nutrientes 21-22 #
# ####################
# nombre_archivo    = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/SOLO_NUTRIENTES/NUTRIENTES-21-22.xlsx'

# datos_radiales = pandas.read_excel(nombre_archivo)
# datos_radiales_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_radiales,direccion_host,base_datos,usuario,contrasena,puerto)  

# # Recupera el identificador del programa de muestreo
# id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra el identificador asociado a cada registro
# print('Asignando el registro correspondiente a cada medida')
# datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_radiales_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
 
# print('Introduciendo los datos en la base de datos')
# FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)
# FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)
  

# ########################################
# # Clorofilas, prod_primaria, cop y nop #
# ########################################

# fecha_umbral = datetime.date(2018,1,1)
# datos_radiales = INSERTA_DATOS_RADIALES_HISTORICO.recupera_id(fecha_umbral,usuario,contrasena,direccion_host,puerto,base_datos)

# # Recupera el identificador del programa de muestreo
# id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra el identificador asociado a cada registro
# print('Asignando el registro correspondiente a cada medida')
# datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_radiales,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
 
# print('Introduciendo los datos en la base de datos')
# FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)
# FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)
  