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



# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'
programa_muestreo = 'RADIAL CORUÑA'

# # # DATOS HISTORICOS (1988 - 2012)
# nombre_archivo    = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/HISTORICO/HISTORICO_MODIFICADO.xlsx'
# print('Añadiendo los datos históricos')   
# INSERTA_DATOS_RADIALES_HISTORICO.inserta_radiales_historico(nombre_archivo,base_datos,usuario,contrasena,puerto,direccion_host,programa_muestreo)


# # RADIALES 2013-2020
directorio_datos           = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES'
# # Listado de archivos disponibles
# from os import listdir
# from os.path import isfile, join
# listado_archivos = [f for f in listdir(directorio_datos) if isfile(join(directorio_datos, f))]

# for iarchivo in range(len(listado_archivos)):
# #for iarchivo in range(0,1):

#     nombre_archivo = directorio_datos + '/' + listado_archivos[iarchivo]

#     #nombre_archivo = directorio_datos + '/RADIAL_BTL_COR_2020.xlsx'
    
#     print('Procesando la informacion correspondiente al año ',nombre_archivo[-9:-5])
    
#     print('Leyendo los datos contenidos en el archivo excel')
#     datos_radiales = FUNCIONES_LECTURA.lectura_datos_radiales(nombre_archivo,direccion_host,base_datos,usuario,contrasena,puerto)
        
#     # Realiza un control de calidad primario a los datos importados   
#     print('Realizando control de calidad')
#     datos_radiales_corregido,textos_aviso = FUNCIONES_PROCESADO.control_calidad(datos_radiales)  
    
#     datos_radiales_corregido = datos_radiales_corregido[datos_radiales_corregido['presion_ctd'].notna()] 
#     datos_radiales_corregido = datos_radiales_corregido[datos_radiales_corregido['fecha_muestreo'].notna()] 
    
#     #datos_radiales_corregido['temperatura_ctd_qf'] = 
    
#     # Recupera el identificador del programa de muestreo
#     id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)
    
#     # Encuentra la estación asociada a cada registro
#     print('Asignando la estación correspondiente a cada medida')
#     datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_estaciones(datos_radiales_corregido,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)
    
#     # Encuentra las salidas al mar correspondientes
#     tipo_salida = 'MENSUAL'   
#     datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_salidas(datos_radiales_corregido,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
     
#     # Encuentra el identificador asociado a cada registro
#     print('Asignando el registro correspondiente a cada medida')
#     datos_radiales_corregido = FUNCIONES_PROCESADO.evalua_registros(datos_radiales_corregido,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
       
#     # # # # # Introduce los datos en la base de datos
#     print('Introduciendo los datos en la base de datos')
#     FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)
#     FUNCIONES_PROCESADO.inserta_datos(datos_radiales_corregido,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)


# DATOS DEL 21-23
# # Parámetros
anhos = [2021]
ruta_archivos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/MENSUALES/Procesados'
import INSERTA_DATOS_PERFILES
for ianho in range(len(anhos)):
    anho = anhos[ianho]
    #INSERTA_DATOS_PERFILES.inserta_radiales_historico(ruta_archivos,anho,programa_muestreo,base_datos,usuario,contrasena,puerto,direccion_host)       

    nombre_programa = programa_muestreo
    from sqlalchemy import create_engine
    import pandas.io.sql as psql
    from glob import glob
    import os
    import numpy


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
    #for isalida in range(len(listado_salidas)):
    for isalida in range(1):    
        
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
                  
            mensaje_error,datos_botellas,io_par,io_fluor,io_O2 = FUNCIONES_LECTURA.lectura_btl(nombre_archivo,datos_archivo)
        
            for imuestreo in range(datos_botellas.shape[0]):
                if datos_botellas['latitud'].iloc[imuestreo] is None:
                    datos_botellas['latitud'].iloc[imuestreo] = tabla_estaciones_programa['latitud_estacion'][tabla_estaciones_programa['id_estacion']==id_estacion].iloc[0]
                if datos_botellas['longitud'].iloc[imuestreo] is None:
                    datos_botellas['longitud'].iloc[imuestreo] = tabla_estaciones_programa['longitud_estacion'][tabla_estaciones_programa['id_estacion']==id_estacion].iloc[0]
        
            # Aplica control de calidad
            #datos_botellas,textos_aviso                = FUNCIONES_PROCESADO.control_calidad(datos_botellas,direccion_host,base_datos,usuario,contrasena,puerto)            
            datos_botellas['id_estacion_temp']         = id_estacion

            datos_botellas['fecha_muestreo']    = fecha_salida
            
            profundidades_referencia = tabla_estaciones_programa['profundidades_referencia'][tabla_estaciones_programa['nombre_estacion']==nombre_estacion].iloc[0]
            # Añade una columna con la profundidad de referencia
            if profundidades_referencia is not None:
                datos_botellas['prof_referencia'] = numpy.zeros(datos_botellas.shape[0],dtype=int)
                for idato in range(datos_botellas.shape[0]):
                        # Encuentra la profundidad de referencia más cercana a cada dato
                        idx = (numpy.abs(profundidades_referencia - datos_botellas['presion_ctd'][idato])).argmin()
                        datos_botellas['prof_referencia'][idato] =  profundidades_referencia[idx]
            else:
                datos_botellas['prof_referencia'] = [None]*datos_botellas.shape[0]

            
          
            # Cambia los nombre de las botellas.        
            if id_estacion == 1: #E2
                listado_equiv_ctd = [1,3,5,7,9,11]
                listado_equiv_real = [1,2,3,4,5,6]
                for ibotella in range(datos_botellas.shape[0]):
                    for iequiv in range(len(listado_equiv_ctd)):
                        if datos_botellas['botella'].iloc[ibotella] == listado_equiv_ctd[iequiv]:
                            datos_botellas['botella'].iloc[ibotella] = listado_equiv_real[iequiv]

            if id_estacion == 5: #E4
                
                datos_botellas['botella_temp'] = datos_botellas['botella']
                datos_botellas = datos_botellas.drop(datos_botellas[datos_botellas.botella_temp == 11].index)
                
                listado_equiv_ctd = [1,3,5,7,9,11]
                listado_equiv_real = [8,9,10,11,12,None]
                for ibotella in range(datos_botellas.shape[0]):
                    for iequiv in range(len(listado_equiv_ctd)):
                        if datos_botellas['botella_temp'].iloc[ibotella] == listado_equiv_ctd[iequiv]:
                            datos_botellas['botella'].iloc[ibotella] = listado_equiv_real[iequiv]
                            
                datos_botellas = datos_botellas.drop(columns=['botella_temp'])  
            
            

            df_acc = pandas.concat([df_acc, datos_botellas])


        
        # Asigna el identificador de la salida al mar
        df_acc['id_salida'] =  id_salida
        
        # Define un nuevo índice de filas. Si se han eliminado registros este paso es necesario
        indices_dataframe        = numpy.arange(0,df_acc.shape[0],1,dtype=int)    
        df_acc['id_temp'] = indices_dataframe
        df_acc.set_index('id_temp',drop=False,append=False,inplace=True)
    
    
        #datos_botellas,textos_aviso = FUNCIONES_PROCESADO.control_calidad(df_acc,direccion_host,base_datos,usuario,contrasena,puerto)  
    
        # Asigna el registro correspondiente a cada muestreo e introduce la información en la base de datos
        datos_botellas = FUNCIONES_PROCESADO.evalua_registros(df_acc,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
     
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
  