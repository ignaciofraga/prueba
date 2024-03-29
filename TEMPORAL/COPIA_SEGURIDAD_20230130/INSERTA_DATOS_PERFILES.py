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
import psycopg2
from glob import glob


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
nombre_programa = 'RADIAL CORUÑA'

anho = 2021
ruta_archivos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/MENSUALES/Procesados'
tipo_salida   = 'MENSUAL' 
configuracion_perfilador = 1

# Recupera el identificador del programa
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto)
            
# recupera la información de las estaciones incluidas en la base de datos
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn_psql)
conn_psql.dispose()

tabla_estaciones_programa = tabla_estaciones[tabla_estaciones['programa']==int(id_programa)]
tabla_salidas_programa    = tabla_salidas[tabla_salidas['programa']==int(id_programa)]

ruta_datos = ruta_archivos + '/' + str(anho) + '/*/'

# Recupera el nombre de los directorios
listado_salidas = glob(ruta_datos, recursive = True)

# Mantén sólo la parte de fechas
for isalida in range(len(listado_salidas)):
#for isalida in range(1):
       
    os.chdir(listado_salidas[isalida])
    for archivo in glob("*.cnv"):
        print(archivo)
        posicion_inicio    = archivo.find('e') + 1
        posicion_final     = archivo.find('.cnv')
        nombre_estacion    = archivo[posicion_inicio:posicion_final].upper() 
        
        id_estacion = tabla_estaciones_programa['id_estacion'][tabla_estaciones_programa['nombre_estacion']==str(nombre_estacion)].iloc[0]
       
        lectura_archivo = open(archivo, "r")  
        datos_archivo = lectura_archivo.readlines()
               
        datos_perfil,listado_variables,fecha_muestreo,hora_muestreo,cast_muestreo = FUNCIONES_LECTURA.lectura_archivo_perfiles(datos_archivo)
        
        df_datos = pandas.DataFrame(datos_perfil, columns = listado_variables)                
  
        # Busca la salida a la que corresponde el muestreo
        id_salida = tabla_salidas_programa['id_salida'][tabla_salidas_programa['fecha_salida']==fecha_muestreo].iloc[0]
      
        # Obtén el identificador del perfil en la base de datos
        instruccion_sql = '''INSERT INTO perfiles_verticales (nombre_perfil,estacion,salida_mar,num_cast,fecha_perfil,hora_perfil,configuracion_perfilador)
        VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_perfil,num_cast,configuracion_perfilador) DO NOTHING;''' 
        
        nombre_perfil = abreviatura_programa + '_' + fecha_muestreo.strftime("%Y%m%d") + '_E' + str(nombre_estacion) + '_C' + str(cast_muestreo)
        
        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
        cursor = conn.cursor()    
        cursor.execute(instruccion_sql,(nombre_perfil,int(id_estacion),int(id_salida),int(cast_muestreo),fecha_muestreo,hora_muestreo,int(configuracion_perfilador)))
        conn.commit() 

        instruccion_sql = "SELECT id_perfil FROM perfiles_verticales WHERE nombre_perfil = '" + nombre_perfil + "';" 
        cursor = conn.cursor()    
        cursor.execute(instruccion_sql)
        id_perfil =cursor.fetchone()[0]
        conn.commit()       
        
     
        # DATOS FISICA
        df_temp            = df_datos[['presion_ctd','temperatura_ctd']]
        df_temp['qf_temp'] = 2
        json_temperatura   = df_temp.to_json()
        
        df_sal            = df_datos[['presion_ctd','salinidad_ctd']]
        df_sal['qf_sal']  = 2
        json_salinidad    = df_sal.to_json() 
        
        instruccion_sql = '''INSERT INTO datos_perfil_fisica (perfil,temperatura_perfil,salinidad_perfil)
        VALUES (%s,%s,%s) ON CONFLICT (perfil) DO UPDATE SET (temperatura_perfil,salinidad_perfil) = ROW(EXCLUDED.temperatura_perfil,EXCLUDED.salinidad_perfil);''' 
        
        datos_insercion = (int(id_perfil),json_temperatura,json_salinidad)
        
        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
        cursor = conn.cursor()    
        cursor.execute(instruccion_sql,datos_insercion)
        conn.commit()  
        
        if 'par_ctd' in listado_variables:
            df_par           = df_datos[['presion_ctd','par_ctd']]
            df_par['qf_par'] = 2
            json_par         = df_par.to_json()   

            instruccion_sql = '''INSERT INTO datos_perfil_fisica (perfil,temperatura_perfil,salinidad_perfil,par_perfil)
            VALUES (%s,%s,%s,%s) ON CONFLICT (perfil) DO UPDATE SET (temperatura_perfil,salinidad_perfil,par_perfil) = ROW(EXCLUDED.temperatura_perfil,EXCLUDED.salinidad_perfil,EXCLUDED.par_perfil);''' 
        
            datos_insercion = (int(id_perfil),json_temperatura,json_salinidad,json_par)
                
            conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
            cursor = conn.cursor()    
            cursor.execute(instruccion_sql,datos_insercion)
            conn.commit()  
            
        
        # DATOS BIOGEOQUIMICA
        if 'oxigeno_ctd' in listado_variables:
            df_oxigeno           = df_datos[['presion_ctd','oxigeno_ctd']]
            df_oxigeno['qf_oxi'] = 2
            json_oxigeno         = df_oxigeno.to_json()

            instruccion_sql = '''INSERT INTO datos_perfil_biogeoquimica (perfil,oxigeno_perfil)
            VALUES (%s,%s) ON CONFLICT (perfil) DO UPDATE SET (oxigeno_perfil) = ROW(EXCLUDED.oxigeno_perfil);''' 
       
            datos_insercion = (int(id_perfil),json_oxigeno)
               
            conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
            cursor = conn.cursor()    
            cursor.execute(instruccion_sql,datos_insercion)
            conn.commit()          
 
        
        if 'fluorescencia_ctd' in listado_variables:
            df_fluor             = df_datos[['presion_ctd','fluorescencia_ctd']]
            df_fluor['qf_fluor'] = 2
            json_fluor           = df_fluor.to_json()

            instruccion_sql = '''INSERT INTO datos_perfil_biogeoquimica (perfil,fluorescencia_perfil)
            VALUES (%s,%s) ON CONFLICT (perfil) DO UPDATE SET (fluorescencia_perfil) = ROW(EXCLUDED.fluorescencia_perfil);''' 
       
            datos_insercion = (int(id_perfil),json_fluor)
               
            conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
            cursor = conn.cursor()    
            cursor.execute(instruccion_sql,datos_insercion)
            conn.commit()  
            
        cursor.close()
        conn.close()  
        
        
    # # Calcula las variaciones de profundidad para saber si el perfil es de subida o bajada
    # df_datos['prof_inc'] = numpy.zeros(df_datos.shape[0])
    # for idato in range(1,df_datos.shape[0]):
    #    df_datos['prof_inc'][idato] =  df_datos['presion_ctd'][idato] - df_datos['presion_ctd'][idato-1]
    
    # df_datos['prof_inc'].iloc[0] = df_datos['prof_inc'].iloc[1]
    # # Asigna a la máxima profunidad una variación de 0 (para que esté en ambos perfiles)
    # df_datos['prof_inc'].iloc[df_datos['presion_ctd'].idxmax()]=0  
    
    # # Extrae los dataframes de subida y bajada
    # df_datos_subida = df_datos[df_datos['prof_inc']<=0]
    # df_datos_bajada = df_datos[df_datos['prof_inc']>=0]