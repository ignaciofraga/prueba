# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""

import numpy
import pandas
import datetime
from pyproj import Proj
import math
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine
import json
import re
#import FUNCIONES_PROCESADO


from matplotlib.ticker import FormatStrFormatter

pandas.options.mode.chained_assignment = None


#############################################################################
######## FUNCION PARA LEER DATOS CON EL FORMATO DE CAMPAÑA RADIALES  ########
############################################################################# 

def lectura_datos_radiales(nombre_archivo,direccion_host,base_datos,usuario,contrasena,puerto):
   
    ## CARGA LA INFORMACION CONTENIDA EN EL EXCEL
    
    # Importa el .xlsx
    datos_radiales = pandas.read_excel(nombre_archivo, 'data')
    
    # Elimina la primera fila, con unidades de las distintas variables
    datos_radiales = datos_radiales.iloc[1: , :]
    
    # Convierte las fechas de DATE a formato correcto
    datos_radiales['DATE'] =  pandas.to_datetime(datos_radiales['DATE'], format='%Y%m%d').dt.date
      
    # Define una columna índice
    indices_dataframe         = numpy.arange(0,datos_radiales.shape[0],1,dtype=int)
    datos_radiales['id_temp'] = indices_dataframe
    datos_radiales.set_index('id_temp',drop=True,append=False,inplace=True)
    
        
    ### IDENTIFICA LAS CONFIGURACIONES  (PERFILADOR Y SUPERFICIE) UTILIZADAS EN CADA REGISTRO
    # Actualmente se hace a partir del buque utilizado. Repensar esto si más adelante un mismo buque 
    # puede (o ha tenido) varias configuraciones 
      
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()
      
    # Datos de los buques oceanograficos incluidos en la base de datos
    instruccion_sql = 'SELECT id_buque FROM buques ;'
    cursor.execute(instruccion_sql)
    id_buque_bd =cursor.fetchall()
    conn.commit()
    instruccion_sql = 'SELECT codigo_buque FROM buques;'
    cursor.execute(instruccion_sql)
    codigo_buque_bd =cursor.fetchall()
    conn.commit()
    conn.close()  
    
    # Compara el codigo de cada registro con el de los buques, para identificar el utilizado en cada registro 
    id_config_sup     = numpy.zeros(datos_radiales.shape[0],dtype=int)
    id_config_perfil  = numpy.zeros(datos_radiales.shape[0],dtype=int)
    for iregistro in range(datos_radiales.shape[0]):
        codigo_buque_registro = datos_radiales['EXPOCODE'][iregistro][2:4]
        for ibuque in range(len(id_buque_bd)):
            if codigo_buque_bd[ibuque][0] == codigo_buque_registro:
                id_config_sup[iregistro] = id_buque_bd[ibuque][0]
                id_config_perfil[iregistro] = id_buque_bd[ibuque][0]
                
        # bucle if específico para la configuración 4 (AJ con SEAPOINT)
        if id_config_perfil[iregistro] == 2 and int(datos_radiales['CTDFLUOR_SP_FLAG_W'][iregistro]) == 2:           
            id_config_perfil[iregistro] = 4
      
    # Asigna el identificador de la configuracion de perfilador y superficie a partir del
    # buque utilizado. 
    datos_radiales['configuracion_superficie'] = id_config_sup
    datos_radiales['configuracion_perfilador'] = id_config_perfil
    
    
    # Determina el origen de los datos de fluorescencia, según la configuración del perfilador
    datos_radiales['fluorescencia_ctd'] = numpy.zeros(datos_radiales.shape[0])
    datos_radiales['fluorescencia_ctd_qf'] = numpy.zeros(datos_radiales.shape[0])
    for idato in range(datos_radiales.shape[0]):
        if datos_radiales['configuracion_perfilador'][idato] == 1:
            datos_radiales['fluorescencia_ctd'][idato]    =  datos_radiales['CTDFLOUR_SCUFA'][idato]
            datos_radiales['fluorescencia_ctd_qf'][idato] = datos_radiales['CTDFLOUR_SCUFA_FLAG_W'][idato]
        if datos_radiales['configuracion_perfilador'][idato] == 2 or datos_radiales['configuracion_perfilador'][idato] == 3:
            datos_radiales['fluorescencia_ctd'][idato]     = datos_radiales['CTDFLUOR_AFL'][idato]
            datos_radiales['fluorescencia_ctd_qf'][idato]  = datos_radiales['CTDFLUOR_AFL_FLAG_W'][idato]
        if datos_radiales['configuracion_perfilador'][idato] == 4:
            datos_radiales['fluorescencia_ctd'][idato]     = datos_radiales['CTDFLUOR_SP'][idato]
            datos_radiales['fluorescencia_ctd_qf'][idato]  = datos_radiales['CTDFLUOR_SP_FLAG_W'][idato]
        
        if datos_radiales['fluorescencia_ctd'][idato] < 0:
            datos_radiales['fluorescencia_ctd'][idato]    = None 
            datos_radiales['fluorescencia_ctd_qf'][idato] = 9    
            
    # Determina el origen de los datos de pH
    datos_radiales['ph']        = numpy.zeros(datos_radiales.shape[0])
    datos_radiales['ph_qf']     = numpy.zeros(datos_radiales.shape[0],dtype=int)
    datos_radiales['ph_metodo'] = numpy.zeros(datos_radiales.shape[0],dtype=int)

    for idato in range(datos_radiales.shape[0]):
        if datos_radiales['PHTS25P0_UNPUR_FLAG_W'][idato] != 9 and  datos_radiales['PHTS25P0_PUR_FLAG_W'][idato] == 9:
            datos_radiales['ph'][idato]        =  datos_radiales['PHTS25P0_UNPUR'][idato]
            datos_radiales['ph_qf'][idato]     = datos_radiales['PHTS25P0_UNPUR_FLAG_W'][idato]
            datos_radiales['ph_metodo'][idato] = 1
        elif datos_radiales['PHTS25P0_UNPUR_FLAG_W'][idato] == 9 and  datos_radiales['PHTS25P0_PUR_FLAG_W'][idato] != 9:
            datos_radiales['ph'][idato]        =  datos_radiales['PHTS25P0_PUR'][idato]
            datos_radiales['ph_qf'][idato]     = datos_radiales['PHTS25P0_PUR_FLAG_W'][idato]
            datos_radiales['ph_metodo'][idato] = 2
        else: 
            datos_radiales['ph'][idato]        =  None
            datos_radiales['ph_qf'][idato]     = 9
            datos_radiales['ph_metodo'][idato] = None
            
              
    for idato in range(datos_radiales.shape[0]):
        if datos_radiales['ph'][idato] is None:
            datos_radiales['ph_metodo'][idato] = None
    
        
    # Asigna el valor del cast. Si es un texto no asigna valor
    datos_radiales['num_cast'] = numpy.ones(datos_radiales.shape[0],dtype=int)
#    datos_radiales['num_cast'] = [None]*datos_radiales.shape[0]
        
    datos_radiales = datos_radiales.drop(columns=['CTDOXY_CAL','CTDOXY_CAL_FLAG_W','CTDFLOUR_SCUFA', 'CTDFLUOR_AFL','CTDFLUOR_SP','CTDFLOUR_SCUFA_FLAG_W','CTDFLUOR_AFL_FLAG_W','CTDFLUOR_SP_FLAG_W','PHTS25P0_UNPUR','PHTS25P0_UNPUR_FLAG_W','PHTS25P0_PUR','PHTS25P0_PUR_FLAG_W'])
     
    # Renombra las columnas para mantener un mismo esquema de nombres   
    datos_radiales = datos_radiales.rename(columns={"DATE": "fecha_muestreo", "STNNBR": "estacion", 'EXPOCODE':'nombre_muestreo',
                                                    "LATITUDE":"latitud","LONGITUDE":"longitud","BTLNBR":"botella","CTDPRS":"presion_ctd",
                                                    "CTDTMP":"temperatura_ctd","CTDSAL":"salinidad_ctd","CTDSAL_FLAG_W":"salinidad_ctd_qf",
                                                    "CTDOXY":"oxigeno_ctd","CTDOXY_FLAG_W":"oxigeno_ctd_qf","CTDPAR":"par_ctd","CTDPAR_FLAG_W":"par_ctd_qf",
                                                    "CTDTURB":"turbidez_ctd","CTDTURB_FLAG_W":"turbidez_ctd_qf","OXYGEN":"oxigeno_wk","OXYGEN_FLAG_W":"oxigeno_wk_qf",
                                                    "SILCAT":"silicato","SILCAT_FLAG_W":"silicato_qf","NITRAT":"nitrato","NITRAT_FLAG_W":"nitrato_qf","NITRIT":"nitrito","NITRIT_FLAG_W":"nitrito_qf",
                                                    "PHSPHT":"fosfato","PHSPHT_FLAG_W":"fosfato_qf","TCARBN":"tcarbn","TCARBN_FLAG_W":"tcarbn_qf","ALKALI":"alcalinidad","ALKALI_FLAG_W":"alcalinidad_qf",                                                   
                                                    "R_CLOR":"r_clor","R_CLOR_FLAG_W":"r_clor_qf","R_PER":"r_per","R_PER_FLAG_W":"r_per_qf","CO3_TMP":"co3_temp"
                                                    })    
    
    datos_radiales['TON']    = datos_radiales['nitrato'] + datos_radiales['nitrito']
    datos_radiales['TON_qf'] = 2
    
    # Añade una columan con el QF de la temperatura, igual al de la salinidad
    datos_radiales['temperatura_ctd_qf'] = datos_radiales['salinidad_ctd_qf'] 
    
    # Añade una columna con la profundidad de referencia
    datos_radiales['prof_referencia'] = numpy.zeros(datos_radiales.shape[0],dtype=int)
    for idato in range(datos_radiales.shape[0]):
        
        # Define las profundidades de referencia en cada estación
        if str(datos_radiales['estacion'][idato]) == '2': #Estación 2
            profundidades_referencia = numpy.asarray([0,5,10,20,30,40,70])
        if str(datos_radiales['estacion'][idato]) == '4': #Estación 2        
            profundidades_referencia = numpy.asarray([0,4,8,12,18]) 
        if str(datos_radiales['estacion'][idato]) == '3c': #Estación 2        
             profundidades_referencia = numpy.asarray([0,5,10,20,30,35,40]) 
             
        # Encuentra la profundidad de referencia más cercana a cada dato
        idx = (numpy.abs(profundidades_referencia - datos_radiales['presion_ctd'][idato])).argmin()
        datos_radiales['prof_referencia'][idato] =  profundidades_referencia[idx]

        
    
    
    return datos_radiales
 
    
 
    
############################################################################
######## FUNCION PARA LEER DATOS CON EL FORMATO DE CAMPAÑA PELACUS  ########
############################################################################ 
    
def lectura_datos_pelacus(nombre_archivo):
  
    # Importa el .xlsx
    datos_pelacus = pandas.read_excel(nombre_archivo, 'tabla_nutrientes',dtype={'hora': datetime.datetime})
    
    # Define una columna índice
    indices_dataframe         = numpy.arange(0,datos_pelacus.shape[0],1,dtype=int)
    datos_pelacus['id_temp'] = indices_dataframe
    datos_pelacus.set_index('id_temp',drop=True,append=False,inplace=True)
    
    # Convierte las fechas de DATE a formato correcto
    datos_pelacus['fecha'] =  pandas.to_datetime(datos_pelacus['fecha'], format='%d%m%Y').dt.date
             
    # Genera una columna con la profundidad. Usar el valor real (si existe) o la teórica en caso contrario
    datos_pelacus['presion_ctd'] = datos_pelacus['Prof_teor.']
    for idato in range(datos_pelacus.shape[0]):
        if numpy.isnan(datos_pelacus['Prof_real'][idato]) is False:
            datos_pelacus['presion_ctd'][idato] = datos_pelacus['Prof_real'][idato]

    # datos_pelacus['presion_ctd'] = numpy.zeros(datos_pelacus.shape[0])
    # for idato in range(datos_pelacus.shape[0]):
    #     if datos_pelacus['Prof_real'][idato] is not None:
    #         datos_pelacus['presion_ctd'][idato] = datos_pelacus['Prof_real'][idato]
    #     if datos_pelacus['Prof_real'][idato] is None and datos_pelacus['Prof_teor.'][idato] is not None:
    #         datos_pelacus['presion_ctd'][idato] = datos_pelacus['Prof_teor.'][idato]        
    #     if datos_pelacus['Prof_real'][idato] is None and datos_pelacus['Prof_teor.'][idato] is None:
    #         datos_pelacus['presion_ctd'][idato] = -999   
            
    # Asigna la profundidad teórica como la de referencia
    datos_pelacus['prof_referencia'] = datos_pelacus['Prof_teor.']
  
    datos_pelacus = datos_pelacus.drop(columns=['Prof_est','Prof_real', 'Prof_teor.'])  
  
    # Asigna el valor del cast. Si es un texto no asigna valor
    
    datos_pelacus['num_cast'] = numpy.ones(datos_pelacus.shape[0],dtype=int)
    for idato in range(datos_pelacus.shape[0]):
        try: # Si es entero
            texto       = datos_pelacus['cast'][idato]      
            datos_pelacus['num_cast'][idato] = int(re.findall(r'\d+',texto)[0])            
        except:
            datos_pelacus['num_cast'][idato] = None

    # # Asigna el identificador de cada muestreo siguiendo las indicaciones de EXPOCODE. 
    # datos_pelacus['nombre_muestreo'] = [None]*datos_pelacus.shape[0]
    # for idato in range(datos_pelacus.shape[0]):    
    #     datos_pelacus['nombre_muestreo'][idato] = '29XX' + datos_pelacus['fecha'][idato].strftime("%Y%m%d")

    # Corrige las horas (diferente formato)
    for idato in range(datos_pelacus.shape[0]): 
        if datos_pelacus['hora'][idato] is not None:
            try: 
                datos_pelacus['hora'][idato] = datetime.datetime.strptime(datos_pelacus['hora'][idato], '%H:%M').time()
            except:
                pass
            
    # Renombra las columnas para mantener una denominación homogénea
    datos_pelacus = datos_pelacus.rename(columns={"campaña":"programa","fecha":"fecha_muestreo","hora":"hora_muestreo","estación":"estacion",
                                                  "Latitud":"latitud","Longitud":"longitud","t_CTD":"temperatura_ctd","Sal_CTD":"salinidad_ctd","SiO2":"silicato","SiO2_flag":"silicato_qf",
                                                  "NO3":"nitrato","NO3T_flag":"nitrato_qf","NO2":"nitrito","NO2_flag":"nitrito_qf","NH4":"amonio","NH4_flag":"amonio_qf","PO4":"fosfato","PO4_flag":"fosfato_qf","Cla":"clorofila_a"
                                                  })

    
    datos_pelacus['temperatura_ctd_qf'] = 9
    datos_pelacus['salinidad_ctd_qf']   = 9
    # Asigna qf a los datos disponibles 
    for idato in range(datos_pelacus.shape[0]):
        if datos_pelacus['temperatura_ctd'][idato]:
            datos_pelacus['temperatura_ctd_qf'][idato]=2
        if datos_pelacus['salinidad_ctd'][idato]:
            datos_pelacus['salinidad_ctd_qf'][idato]=2            


    return datos_pelacus






############################################################################
######## FUNCION PARA LEER DATOS CON EL FORMATO DE CAMPAÑA RADPROF  ########
############################################################################ 
    
def lectura_datos_radprof(nombre_archivo):
  
    # Importa el .xlsx
    datos_radprof = pandas.read_excel(nombre_archivo,skiprows=1)
    
    # Define una columna índice
    indices_dataframe         = numpy.arange(0,datos_radprof.shape[0],1,dtype=int)
    datos_radprof['id_temp'] = indices_dataframe
    datos_radprof.set_index('id_temp',drop=True,append=False,inplace=True)
       
    # Cambia, en el dataframe, una única columna de fecha/hora por dos columnas: una de fecha y otra de hora
    datos_radprof['fecha_muestreo'] = [None]*datos_radprof.shape[0]
    datos_radprof['hora_muestreo']  = [None]*datos_radprof.shape[0]
    for idato in range(datos_radprof.shape[0]):
        datos_radprof['fecha_muestreo'][idato] = (datos_radprof['Date'][idato]).date()
        datos_radprof['hora_muestreo'][idato]  = (datos_radprof['Date'][idato]).time()
        datos_radprof['st'][idato]             = str(datos_radprof['st'][idato])
        
    # Calcula la columna de NO3 a partir de la suma NO3+NO2
    datos_radprof['nitrato']    =  datos_radprof['NO3+NO2 umol/Kg'] - datos_radprof['NO2 umol/kg']
    datos_radprof['nitrato_qf'] =  datos_radprof['Flag_TON'] 
       

    # Renombra las columnas para mantener una denominación homogénea
    datos_radprof = datos_radprof.rename(columns={"st":"estacion","Niskin":"botella","Cast":'num_cast',
                                                  "Lat":"latitud","Lon":"longitud","CTDPRS":"presion_ctd","CTDtemp":"temperatura_ctd","SALCTD":"salinidad_ctd",
                                                  "SiO2 umol/Kg":"silicato","Flag_SiO2":"silicato_qf",
                                                  "NO2 umol/kg":"nitrito","Flag_NO2":"nitrito_qf","PO4 umol/Kg":"fosfato","Flag_PO4":"fosfato_qf"
                                                  })

    # Mantén solo las columnas que interesan
    datos_radprof_recorte = datos_radprof[['estacion','botella','fecha_muestreo','hora_muestreo','latitud','longitud','presion_ctd','num_cast','temperatura_ctd','salinidad_ctd',
                                            'nitrato','nitrato_qf','nitrito','nitrito_qf','silicato','silicato_qf','fosfato','fosfato_qf']]
    

    # # Renombra las columnas para mantener una denominación homogénea
    # datos_radprof = datos_radprof.rename(columns={"st":"estacion","Niskin":"botella","Cast":'num_cast',
    #                                               "Lat":"latitud","Lon":"longitud","CTDPRS":"presion_ctd","CTDtemp":"temperatura_ctd","SALCTD":"salinidad_ctd",
    #                                               "silica":"silicato","SiO2_flag":"silicato_qf","nitra":"nitrato","NO3_flag":"nitrato_qf",
    #                                               "nitri":"nitrito","NO2_flag":"nitrito_qf","phospha":"fosfato","PO4_flag":"fosfato_qf","ammonia":"amonio"
    #                                               })   
    
    # # Mantén solo las columnas que interesan
    # datos_radprof_recorte = datos_radprof[['estacion','botella','fecha_muestreo','hora_muestreo','latitud','longitud','presion_ctd','num_cast','temperatura_ctd','salinidad_ctd',
    #                                        'nitrato','nitrato_qf','nitrito','nitrito_qf','silicato','silicato_qf','fosfato','fosfato_qf','amonio','configuracion_superficie','configuracion_perfilador']]

    del(datos_radprof)

    return datos_radprof_recorte



############################################################################
######## FUNCION PARA LEER DATOS CON EL FORMATO DE CAMPAÑA RADPROF  ########
############################################################################ 
    
def lectura_archivo_perfiles(datos_archivo):

    # Predimensionamientos
    listado_variables  = []
    io_datos           = 0
    
    # Lee el archivo .cnv
    cast_muestreo      = 1 # Asinga este valor por si no se introdujo ningún dato en el muestreo
    fecha_muestreo     = None
    hora_muestreo      = None
    for ilinea in range(len(datos_archivo)):
        texto_linea = datos_archivo[ilinea]
        if io_datos == 0:            
            if texto_linea[0:8] == '** Time:': # Línea con hora del cast
                hora_muestreo = datetime.datetime.strptime(texto_linea[8:13],'%H:%M').time() 
            if texto_linea[0:8] == '** Cast:': # Línea con el número de cast
                cast_muestreo = int(texto_linea[8:12])
            if texto_linea[0:8] == '** Date:': # Línea con la fecha
                fecha_muestreo = texto_linea[8:16]
                try: 
                    fecha_muestreo = datetime.datetime.strptime(fecha_muestreo, '%d/%m/%y').date()
                except:
                    pass
                
            if texto_linea[0:6] == '# name': # Línea con variable muestreada
                posicion_inicio    = texto_linea.find('=') + 2
                posicion_final     = texto_linea.find(':')
                nombre_variable    = texto_linea[posicion_inicio:posicion_final]
                
                if nombre_variable == 'prSM':
                    nombre_variable = 'presion_ctd'
                if nombre_variable == 't090C':
                    nombre_variable = 'temperatura_ctd'
                if nombre_variable == 'sal00':
                    nombre_variable = 'salinidad_ctd'
                if nombre_variable == 'flScufa':
                    nombre_variable = 'fluorescencia_ctd' 
                if nombre_variable == 'par':
                    nombre_variable = 'par_ctd' 
                if nombre_variable == 'sbeox0Mm/Kg': 
                    nombre_variable = 'oxigeno_ctd'                    
                
                listado_variables  = listado_variables + [nombre_variable]
                
            if texto_linea[0:5] == '*END*': # Línea final de encabezado, a partir de aquí, datos
                datos_perfil     = []
                io_datos         = 1
        
        else:

            datos_linea = texto_linea.split() 
            listado_datos = [float(x) for x in datos_linea] 
            datos_perfil.append(listado_datos) 
            
    return datos_perfil,listado_variables,fecha_muestreo,hora_muestreo,cast_muestreo

# ##########################################################
# ######## FUNCION PARA LEER ESTADILLOS DE ENTRADA  ########
# ########################################################## 
    
# def entrada_datos_excel(nombre_archivo):

    
#     # Importa el .xlsx
#     datos_entrada = pandas.read_excel(nombre_archivo, 'datos',dtype={'hora_muestreo': datetime.time})
    
#     ## Comprueba el formato de los datos
    
#     # # Verifica si están todas las columnas, comparando con una plantilla
#     # datos_plantilla = pandas.read_excel(nombre_plantilla, 'datos') 
    
#     # if pandas.Series(datos_entrada.columns).isin(datos_plantilla.columns).all() is False:
#     #     texto_error = 'Los datos de entrada no se ajustan a la plantilla. Revisar el formato del archivo'
    
#     # else:
#     #     texto_error = []
#     texto_error = []
    
#     # corrije el formato de las fechas
#     for idato in range(datos_entrada.shape[0]):
#         datos_entrada['fecha_muestreo'][idato] = (datos_entrada['fecha_muestreo'][idato]).date()
        
#     # Añade el identificador de la configuración del perfilador y la superficie (darle una vuelta a esto)
#     datos_entrada['configuracion_perfilador'] = numpy.ones(datos_entrada.shape[0])
#     datos_entrada['configuracion_superficie'] = numpy.ones(datos_entrada.shape[0])

#     return datos_entrada,texto_error 










######################################################################
######## FUNCION PARA LEER DATOS DE BOTELLAs (ARCHIVOS .BTL)  ########
######################################################################
def lectura_btl(nombre_archivo,datos_archivo,nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto):
 
    
    # recupera la información de las estaciones incluidas en la base de datos
    con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql        = create_engine(con_engine)
    tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
    df_programas = psql.read_sql('SELECT * FROM programas', conn_psql)
    conn_psql.dispose()
    
    id_programa_elegido = df_programas['id_programa'][df_programas['nombre_programa']==nombre_programa].iloc[0]
    
    df_estaciones_radiales = tabla_estaciones[tabla_estaciones['programa']==id_programa_elegido]
    
    
    
    # Identifica la estación a la que corresponde el archivo
    posicion_inicio    = nombre_archivo.find('e') + 1
    posicion_final     = nombre_archivo.find('.')
    nombre_estacion    = nombre_archivo[posicion_inicio:posicion_final].upper() #+ 'CO'                
    id_estacion        = df_estaciones_radiales['id_estacion'][df_estaciones_radiales['nombre_estacion']==nombre_estacion].iloc[0] 
    
    # Identifica la fecha del muestreo
    fecha_salida_texto = nombre_archivo[0:8]
    fecha_salida       = datetime.datetime.strptime(fecha_salida_texto, '%Y%m%d').date()
    
    
    id_estacion              = tabla_estaciones['id_estacion'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]
    profundidades_referencia = tabla_estaciones['profundidades_referencia'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]
    lat_estacion             = tabla_estaciones['latitud_estacion'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]
    lon_estacion             = tabla_estaciones['longitud_estacion'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]
    
    
    # Genera las listas en las que se guardarán los datos si éstos existen
    datos_botella     = []
    datos_salinidad   = []
    datos_temperatura = []
    datos_presion     = []
    datos_PAR         = []
    datos_fluor       = []
    datos_O2          = []
    datos_tiempos     = []

    # Lee el archivo .btl y escribe la información de las botellas en un archivo temporal
    cast_muestreo          = 1 # Asinga este valor por si no se introdujo ningún dato en el muestreo
    fecha_muestreo_archivo = None
    for ilinea in range(len(datos_archivo)):
        texto_linea = datos_archivo[ilinea]
        if texto_linea[0:1] == '#' or texto_linea[0:1] == '*':
            

            if texto_linea[0:12] == '** Latitude:': # Línea con latitud del muestreo
                lat_muestreo  = float(texto_linea[12:19])
                if texto_linea[-1] == 'S':
                    lat_muestreo = lat_muestreo*-1

            if texto_linea[0:13] == '** Longitude:': # Línea con latitud del muestreo
                lon_muestreo  = float(texto_linea[13:20])
                if texto_linea[-1] == 'W':
                    lon_muestreo = lon_muestreo*-1
                    
            if texto_linea[0:22] == '* System UpLoad Time =': # Línea con hora del cast
                listado_textos   = texto_linea.split('= ') 
                datetime_sistema = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S')
                            
            if texto_linea[0:14] == '* System UTC =': # Línea con hora del cast
                listado_textos    = texto_linea.split('= ')  
                datetime_muestreo = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S')
                offset_tiempo     = (datetime_sistema - datetime_muestreo).seconds


            if texto_linea[0:8] == '** Cast:': # Línea con el número de cast
#                cast_muestreo = int(texto_linea[8:len(texto_linea)])
                cast_muestreo = int(texto_linea[8:12])
            if texto_linea[0:8] == '** Date:': # Línea con la fecha
#                fecha_muestreo_archivo = texto_linea[8:len(texto_linea)]
                fecha_muestreo_archivo = texto_linea[8:16]
                try: 
                    fecha_muestreo_archivo = datetime.datetime.strptime(fecha_muestreo_archivo, '%d/%m/%y').date()
                except:
                    pass
    
        else:
    
            # Separa las cabeceras de las medidas de oxigeno si existen y están juntas 
            if 'Sbeox0VSbeox0Mm/Kg' in texto_linea: 
                texto_linea = texto_linea.replace('Sbeox0VSbeox0Mm/Kg', 'Sbeox0V Sbeox0Mm/Kg')
               
            datos_linea = texto_linea.split()
                
            if datos_linea[0] == 'Bottle': # Primera línea con las cabeceras
    
                # Encuentra los indices (posiciones) de cada variable, si ésta está incluida
                indice_botellas  = datos_linea.index("Bottle")
        
                try:
                    indice_salinidad = datos_linea.index("Sal00")            
                except:
                    indice_salinidad = None
               
                try: 
                   indice_presion   = datos_linea.index("PrSM")
                except:
                    indice_presion  = None      
                
                try:
                    indice_temp     = datos_linea.index("T090C")
                except:
                    indice_temp     = None
                    
                try:
                    indice_par      = datos_linea.index("Par")
                    io_par          = 1
                except:
                    indice_par      = None
                    io_par          = 0
                  
                try:
                    indice_fluor    = datos_linea.index("FlScufa")  
                    io_fluor        = 1
                except:
                    indice_fluor    =  None 
                    io_fluor        = 0                    
                
                try:
                    indice_O2       = datos_linea.index("Sbeox0Mm/Kg")
                    io_O2           = 1                   
                except:
                    indice_O2       =  None  
                    io_O2           = 0     
    
    
            elif datos_linea[0] == 'Position': # Segunda línea con las cabeceras
                datos_linea = texto_linea.split() 
                
            else:  # Líneas con datos
                
                datos_linea = texto_linea.split()
                
                if datos_linea[-1] == '(avg)': # Línea con los registros de cada variable
                        
                    # Salvo en el caso del identificador de las botellas, sumar dos espacios al índice de cada variable
                    # porque la fecha la divide en 3 lecturas debido al espacio que contiene
                    
                    datos_botella.append(int(datos_linea[indice_botellas]))
                    datos_salinidad.append(float(datos_linea[indice_salinidad + 2])) 
                    datos_temperatura.append(float(datos_linea[indice_temp + 2]))
                    datos_presion.append(round(float(datos_linea[indice_presion + 2]),2))
                
    
                
                    if io_par == 1:
                        datos_PAR.append(float(datos_linea[indice_par + 2]))
                    if io_fluor == 1:
                        datos_fluor.append(float(datos_linea[indice_fluor + 2]))
                    if io_O2 == 1:
                        datos_O2.append(float(datos_linea[indice_O2 + 2]))                    
                                    
                else: # Linea con los tiempos de cierre
                
                    hora_ciere                = datetime.datetime.strptime(datos_linea[0],'%H:%M:%S').time() 
                    datetime_cierre           = datetime.datetime.combine(datetime_muestreo.date(),hora_ciere)
                    datetime_cierre_corregido = datetime_cierre - datetime.timedelta(seconds=offset_tiempo)    
                    hora_ciere_corregida      = datetime_cierre_corregido.time()
                    datos_tiempos.append(hora_ciere_corregida)
    
    
    # Comprueba que la fecha contenida en el archivo y la del nombre del archivo son la misma
    if fecha_muestreo_archivo is not None and fecha_muestreo_archivo != fecha_salida:
    
        mensaje_error  = 'La fecha indicada en el nombre del archivo no coincide con la que figura en dentro del mismo'
        datos_botellas = None
        io_par         = 0
        io_fluor       = 0
        io_O2          = 0
    
    else:
        
        # Une las listas en un dataframe
        datos_botellas = pandas.DataFrame(list(zip(datos_botella, datos_tiempos,datos_salinidad,datos_temperatura,datos_presion)),
                       columns =['botella', 'hora_muestreo' , 'salinidad_ctd','temperatura_ctd','presion_ctd'])
        
        # Añade columnas con el QF de T y S
        datos_botellas['temperatura_ctd_qf'] = 1
        datos_botellas['salinidad_ctd_qf']   = 1
        
        if io_par == 1:
            datos_botellas['par_ctd']              =  datos_PAR
            datos_botellas['par_ctd_qf']           = 1
        if io_fluor == 1:
            datos_botellas['fluorescencia_ctd']    =  datos_fluor
            datos_botellas['fluorescencia_ctd_qf'] = 1
        if io_O2 == 1:
            datos_botellas['oxigeno_ctd']          =  datos_O2
            datos_botellas['oxigeno_ctd_qf']       = 1
            
            
        # Añade una columna con la profundidad de referencia
        if profundidades_referencia is not None:
            datos_botellas['prof_referencia'] = numpy.zeros(datos_botellas.shape[0],dtype=int)
            for idato in range(datos_botellas.shape[0]):
                    # Encuentra la profundidad de referencia más cercana a cada dato
                    idx = (numpy.abs(profundidades_referencia - datos_botellas['presion_ctd'][idato])).argmin()
                    datos_botellas['prof_referencia'][idato] =  profundidades_referencia[idx]
        else:
            datos_botellas['prof_referencia'] = [None]*datos_botellas.shape[0]
        
        
        # Añade informacion de lat/lon y fecha para que no elimine el registro durante el control de calidad
        datos_botellas['latitud']                  = lat_muestreo  
        datos_botellas['longitud']                 = lon_muestreo 
        datos_botellas['id_estacion_temp']         = numpy.zeros(datos_botellas.shape[0],dtype=int)
        datos_botellas['id_estacion_temp']         = id_estacion
        datos_botellas['estacion']                 = id_estacion
        datos_botellas['fecha_muestreo']           = fecha_muestreo_archivo
        datos_botellas['num_cast']                 = cast_muestreo
        datos_botellas['programa']                 = id_programa_elegido
        
        mensaje_error = []
        
    return mensaje_error,datos_botellas,io_par,io_fluor,io_O2







  
 
 
    
    
