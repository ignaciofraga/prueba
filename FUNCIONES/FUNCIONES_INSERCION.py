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
            datos_radiales['ph'][idato] =  datos_radiales['PHTS25P0_UNPUR'][idato]
            datos_radiales['ph_qf']     = datos_radiales['PHTS25P0_UNPUR_FLAG_W'][idato]
            datos_radiales['ph_metodo'] = 1
        elif datos_radiales['PHTS25P0_UNPUR_FLAG_W'][idato] == 9 and  datos_radiales['PHTS25P0_PUR_FLAG_W'][idato] != 9:
            datos_radiales['ph'][idato] =  datos_radiales['PHTS25P0_PUR'][idato]
            datos_radiales['ph_qf']     = datos_radiales['PHTS25P0_PUR_FLAG_W'][idato]
            datos_radiales['ph_metodo'] = 2
        else: 
            datos_radiales['ph'][idato] =  None
            datos_radiales['ph_qf']     = 9
            datos_radiales['ph_metodo'] = None
            
              
    
        
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
          
    # Añade información de la configuración de perfilador y superficie (TEMPORAL!!!!)
    datos_pelacus['configuracion_superficie'] = numpy.ones(datos_pelacus.shape[0],dtype=int)
    datos_pelacus['configuracion_perfilador'] = numpy.ones(datos_pelacus.shape[0],dtype=int)
    
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
    
#    import re
#>>> re.findall(r'\d+', "hello 42 I'm a 32 string 30")

    datos_pelacus['num_cast'] = numpy.ones(datos_pelacus.shape[0],dtype=int)
    for idato in range(datos_pelacus.shape[0]):
        try: # Si es entero
            texto       = datos_pelacus['cast'][idato]      
            datos_pelacus['num_cast'][idato] = int(re.findall(r'\d+',texto)[0])            
        except:
            datos_pelacus['num_cast'][idato] = None

    # Asigna el identificador de cada muestreo siguiendo las indicaciones de EXPOCODE. 
    datos_pelacus['nombre_muestreo'] = [None]*datos_pelacus.shape[0]
    for idato in range(datos_pelacus.shape[0]):    
        datos_pelacus['nombre_muestreo'][idato] = '29XX' + datos_pelacus['fecha'][idato].strftime("%Y%m%d")

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

    return datos_pelacus






############################################################################
######## FUNCION PARA LEER DATOS CON EL FORMATO DE CAMPAÑA RADPROF  ########
############################################################################ 
    
def lectura_datos_radprof(nombre_archivo):
  
    # Importa el .xlsx
    datos_radprof = pandas.read_excel(nombre_archivo, 'DatosFinales',skiprows=1)
    
    # Define una columna índice
    indices_dataframe         = numpy.arange(0,datos_radprof.shape[0],1,dtype=int)
    datos_radprof['id_temp'] = indices_dataframe
    datos_radprof.set_index('id_temp',drop=True,append=False,inplace=True)
    
    # Añade información de la configuración de perfilador y superficie (TEMPORAL!!!!)
    datos_radprof['configuracion_superficie'] = numpy.ones(datos_radprof.shape[0],dtype=int)
    datos_radprof['configuracion_perfilador'] = numpy.ones(datos_radprof.shape[0],dtype=int)
    
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
                                           'nitrato','nitrato_qf','nitrito','nitrito_qf','silicato','silicato_qf','fosfato','fosfato_qf','configuracion_superficie','configuracion_perfilador']]

    del(datos_radprof)

    return datos_radprof_recorte





##########################################################
######## FUNCION PARA LEER ESTADILLOS DE ENTRADA  ########
########################################################## 
    
def lectura_datos_estadillo(nombre_archivo,nombre_plantilla):

    
    # Importa el .xlsx
    datos_entrada = pandas.read_excel(nombre_archivo, 'datos',dtype={'hora_muestreo': datetime.time})
    
    ## Comprueba el formato de los datos
    
    # Verifica si están todas las columnas, comparando con una plantilla
    datos_plantilla = pandas.read_excel(nombre_plantilla, 'datos') 
    
    if pandas.Series(datos_entrada.columns).isin(datos_plantilla.columns).all() is False:
        texto_error = 'Los datos de entrada no se ajustan a la plantilla. Revisar el formato del archivo'
    
    else:
        texto_error = []
    
    # corrije el formato de las fechas
    for idato in range(datos_entrada.shape[0]):
        datos_entrada['fecha_muestreo'][idato] = (datos_entrada['fecha_muestreo'][idato]).date()
        
    # Añade el identificador de la configuración del perfilador y la superficie (darle una vuelta a esto)
    datos_entrada['configuracion_perfilador'] = numpy.ones(datos_entrada.shape[0])
    datos_entrada['configuracion_superficie'] = numpy.ones(datos_entrada.shape[0])

    return datos_entrada,texto_error 






#######################################################################################
######## FUNCION PARA APLICAR CONTROL DE CALIDAD BÁSICO LOS DATOS DE PROGRAMAS  #######
#######################################################################################
def control_calidad(datos,direccion_host,base_datos,usuario,contrasena,puerto):
 
    textos_aviso = [] 
    
    # Recupera la tabla con los registros de muestreos físicos
    con_engine         = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql          = create_engine(con_engine)
    tabla_variables    = psql.read_sql('SELECT * FROM variables_procesado', conn_psql)  
    conn_psql.dispose()
        
    # Lee las variables de cada tipo a utilizar en el control de calidad
    variables_muestreo = [x for x in tabla_variables['parametros_muestreo'] if str(x) != 'None']
    variables_fisicas  = [x for x in tabla_variables['variables_fisicas'] if str(x) != 'None']    
    variables_biogeoquimicas  = [x for x in tabla_variables['variables_biogeoquimicas'] if str(x) != 'None'] 
        
    listado_completo = variables_muestreo
    
    # Comprueba que las variables relacionadas con el muestreo están incluidas,añadiéndolas si no es así.
    for ivariable_muestreo in range(len(variables_muestreo)):
        if variables_muestreo[ivariable_muestreo] not in datos:
            datos[variables_muestreo[ivariable_muestreo]] = None 
            
    # Mismo proceso para variables físicas existen,  
    for ivariable_fisica in range(len(variables_fisicas)):
        if variables_fisicas[ivariable_fisica] not in datos:
            datos[variables_fisicas[ivariable_fisica]] = None 
           
        # Comprueba si ya existe la variable de control de calidad, si no es así añadirlo a la tabla 
        qf_variable = variables_fisicas[ivariable_fisica] + '_qf'
        if qf_variable not in datos:
            datos[qf_variable] = 0
            datos.astype({qf_variable : 'int'}).dtypes
            datos[qf_variable] = None 
        
        listado_completo.extend([variables_fisicas[ivariable_fisica],qf_variable])
            
    # Mismo proceso para variables biogeoquimicas
    for ivariable_biogeoquimica in range(len(variables_biogeoquimicas)):
        if variables_biogeoquimicas[ivariable_biogeoquimica] not in datos:
            datos[variables_biogeoquimicas[ivariable_biogeoquimica]] = None 
        
        # Comprueba si ya existe la variable de control de calidad, si no es así añadirlo a la tabla 
        qf_variable = variables_biogeoquimicas[ivariable_biogeoquimica] + '_qf'
        if qf_variable not in datos:
            datos[qf_variable] = 0
            datos.astype({qf_variable : 'int'}).dtypes
            datos[qf_variable] = None 
    
        listado_completo.extend([variables_biogeoquimicas[ivariable_biogeoquimica],qf_variable])
        
    # Mismo proceso para las variables pH_metodo y cc_nutrientes
    variables_control = ['ph_metodo','cc_nutrientes']
    for ivariable_control in range(len(variables_control)):

        if variables_control[ivariable_control] not in datos:

            datos[variables_control[ivariable_control]] = None 
            
        listado_completo.extend([variables_control[ivariable_control]])
    
    
    # Reordena las columnas del dataframe para que tengan el mismo orden que el listado de variables
    datos = datos.reindex(columns=listado_completo)
    
    datos = datos.replace({numpy.nan:None})
    
    # Eliminar los registros sin dato de latitud,longitud, profundidad o fecha 
    datos = datos[datos['latitud'].notna()]
    datos = datos[datos['longitud'].notna()]  
    datos = datos[datos['presion_ctd'].notna()] 
    datos = datos[datos['fecha_muestreo'].notna()] 
    
    # Elimina los registros con datos de profundidad negativos
    datos = datos.drop(datos[datos.presion_ctd < 0].index)
    
    # Elimina registros duplicados en el mismo punto y a la misma hora(por precaucion)
    num_reg_inicial = datos.shape[0]
    datos           = datos.drop_duplicates(subset=['latitud','longitud','presion_ctd','fecha_muestreo','hora_muestreo'], keep='last')    
    num_reg_final   = datos.shape[0]
    if num_reg_final < num_reg_inicial:
        textos_aviso.append('Se han eliminado registros correspondientes a una misma fecha, hora, profundidad y estación')
    
    
    # Corregir los valores positivos de longitud, pasándolos a negativos (algunos datos de Pelacus tienen este error)
    datos['longitud'] = -1*datos['longitud'].abs()  
    
    # Define un nuevo índice de filas. Si se han eliminado registros este paso es necesario
    indices_dataframe        = numpy.arange(0,datos.shape[0],1,dtype=int)    
    datos['id_temp'] = indices_dataframe
    datos.set_index('id_temp',drop=False,append=False,inplace=True)
    
    # Redondea los decimales los datos de latitud, longitud y profundidad (precisión utilizada en la base de datos)
    for idato in range(datos.shape[0]):
        datos['longitud'][idato] = round(datos['longitud'][idato],4)
        datos['latitud'][idato] = round(datos['latitud'][idato],4)
        datos['presion_ctd'][idato] = round(datos['presion_ctd'][idato],2)      

    # # # Cambia los valores -999 por None y asigna bandera de calidad correspondiente (por precaucion)
    # # Variables fisicas
    
    for ivariable_fisica in range(len(variables_fisicas)):
        # Valores negativos asigna None
        datos[variables_fisicas[ivariable_fisica]][datos[variables_fisicas[ivariable_fisica]]<0] = None
        # Valores nulos asigna un 9 en el QF
        qf_variable = variables_fisicas[ivariable_fisica] + '_qf'
        datos.loc[datos[variables_fisicas[ivariable_fisica]].isnull(),qf_variable] = 9

    for ivariable_biogeoquimica in range(len(variables_biogeoquimicas)):
        # Valores negativos asigna None
        datos[variables_biogeoquimicas[ivariable_biogeoquimica]][datos[variables_biogeoquimicas[ivariable_biogeoquimica]]<0] = None
        # Valores nulos asigna un 9 en el QF
        qf_variable = variables_biogeoquimicas[ivariable_biogeoquimica] + '_qf'
        datos.loc[datos[variables_biogeoquimicas[ivariable_biogeoquimica]].isnull(),qf_variable] = 9


    return datos,textos_aviso    
 
    
 

 
  
##############################################################################
######## FUNCION PARA ENCONTRAR LA ESTACIÓN ASOCIADA A CADA REGISTRO  ########
##############################################################################

def evalua_estaciones(datos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto):

    con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql        = create_engine(con_engine)
    tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql) 
    
    # Recorta el dataframe para tener sólo las estaciones del programa seleccionado
    estaciones_programa            = tabla_estaciones[tabla_estaciones['programa'] == id_programa]
    indices_dataframe              = numpy.arange(0,estaciones_programa.shape[0],1,dtype=int)    
    estaciones_programa['id_temp'] = indices_dataframe
    estaciones_programa.set_index('id_temp',drop=True,append=False,inplace=True)
    
    ## Identifica la estación asociada a cada registro
    
    # Columna para punteros de estaciones
    datos['id_estacion_temp'] = numpy.zeros(datos.shape[0],dtype=int) 
    
    # Genera un dataframe con la combinación única de latitud/longitud en los muestreos
    estaciones_muestradas                      = datos.groupby(['latitud','longitud']).size().reset_index().rename(columns={0:'count'})
    estaciones_muestradas['id_estacion']           = numpy.zeros(estaciones_muestradas.shape[0],dtype=int)
    estaciones_muestradas['io_nueva_estacion'] = numpy.ones(estaciones_muestradas.shape[0],dtype=int)
    estaciones_muestradas['nombre_estacion']   = [None]*estaciones_muestradas.shape[0]
    
    # Comprueba que las estaciones no están próximas entre sí (y son la misma pero con pequeñas diferencias en las coordenadas)
    proy_datos = Proj(proj='utm',zone=29,ellps='WGS84', preserve_units=False) # Referencia coords
    dist_min   = 750
    
    for iestacion in range(estaciones_muestradas.shape[0]-1):
        x_ref, y_ref = proy_datos(estaciones_muestradas['longitud'][iestacion], estaciones_muestradas['latitud'][iestacion], inverse=False)
        for isiguientes in range(iestacion+1,estaciones_muestradas.shape[0]):
            x_compara, y_compara  = proy_datos(estaciones_muestradas['longitud'][isiguientes], estaciones_muestradas['latitud'][isiguientes], inverse=False)
            distancia             = math.sqrt((((x_ref-x_compara)**2) + ((y_ref-y_compara)**2)))
    
            if distancia < dist_min:
                aux = (datos ['latitud'] == estaciones_muestradas['latitud'][isiguientes]) & (datos ['longitud'] == estaciones_muestradas['longitud'][isiguientes])
                if any(aux) is True:
                    indices_datos = [i for i, x in enumerate(aux) if x]
                    datos['latitud'][indices_datos] = estaciones_muestradas['latitud'][iestacion]
                    datos['longitud'][indices_datos] = estaciones_muestradas['longitud'][iestacion]            
       
    # vuelve a generar el dataframe de muestreos (por si había estaciones iguales)
    del(estaciones_muestradas)
    estaciones_muestradas                      = datos.groupby(['latitud','longitud']).size().reset_index().rename(columns={0:'count'})
    estaciones_muestradas['id_estacion']       = numpy.zeros(estaciones_muestradas.shape[0],dtype=int)
    estaciones_muestradas['io_nueva_estacion'] = numpy.ones(estaciones_muestradas.shape[0],dtype=int)
    estaciones_muestradas['nombre_estacion']   = [None]*estaciones_muestradas.shape[0]
  
    
    if len(tabla_estaciones['id_estacion'])>0:
        id_ultima_estacion_bd = max(tabla_estaciones['id_estacion'])
    else:
        id_ultima_estacion_bd = 0
        
    # Encuentra el nombre asociado a cada par de lat/lon y si está incluida en la base de datos (io_nueva = 0 ya incluida en la base de datos; 1 NO incluida en la base de datos)
    for idato in range(estaciones_muestradas.shape[0]):
        
        # Encuentra el nombre asignado en los datos importados a cada nueva estación 
        aux = (datos ['latitud'] == estaciones_muestradas['latitud'][idato]) & (datos ['longitud'] == estaciones_muestradas['longitud'][idato])
        if any(aux) is True:
            indices_datos = [i for i, x in enumerate(aux) if x]
            estaciones_muestradas['nombre_estacion'][idato]  = datos['estacion'][indices_datos[0]]
    
        # comprueba si la estación muestreada está entre las incluidas en la base de datos (dentro del programa correspondiente)
        aux = (estaciones_programa['latitud'] == estaciones_muestradas['latitud'][idato]) & (estaciones_programa['longitud'] == estaciones_muestradas['longitud'][idato])
        # Si la estación muestreada está incluida, asigna a la estación muestreada el identificador utilizado en la base de datos
        if any(aux) is True:
            indices = [i for i, x in enumerate(aux) if x]
            estaciones_muestradas['io_nueva_estacion'][idato]  = 0
            
            estaciones_muestradas['id_estacion'][idato]  = estaciones_programa['id_estacion'][indices[0]]
        # Si no está incluida, continúa con la numeración de las estaciones e inserta un nuevo registro en la base de datos
        else:
            id_ultima_estacion_bd                    = id_ultima_estacion_bd + 1
            estaciones_muestradas['id_estacion'][idato]  = id_ultima_estacion_bd 
                 
        # Asigna a la matriz de datos la estación asociada a cada registro
        datos['id_estacion_temp'][indices_datos] = estaciones_muestradas['id_estacion'][idato]
    
    
    if numpy.count_nonzero(estaciones_muestradas['io_nueva_estacion']) > 0:
    
        # Genera un dataframe sólo con los valores nuevos, a incluir (io_nuevo_muestreo = 1)
        nuevos_muestreos  = estaciones_muestradas[estaciones_muestradas['io_nueva_estacion']==1]
        # Mantén sólo las columnas que interesan
        exporta_registros = nuevos_muestreos[['id_estacion','nombre_estacion','latitud','longitud']]
        # Añade columna con el identiicador del programa
        exporta_registros['programa'] = numpy.zeros(exporta_registros.shape[0],dtype=int)
        exporta_registros['programa'] = id_programa
        # corrije el indice del dataframe 
        exporta_registros.set_index('id_estacion',drop=True,append=False,inplace=True)
    
        # Inserta el dataframe resultante en la base de datos 
        exporta_registros.to_sql('estaciones', conn_psql,if_exists='append')

    # elimina la informacion cargada y que no se vaya a exportar, para liberar memoria
    del(estaciones_muestradas,estaciones_programa,tabla_estaciones)
    
    conn_psql.dispose() # Cierra la conexión con la base de datos 
    
    return datos  





###############################################################################
###### FUNCION PARA ENCONTRAR LA SALIDA AL MAR ASOCIADA A CADA REGISTRO  ######
###############################################################################

def evalua_salidas(datos,id_programa,nombre_programa,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto):

    # Recupera la tabla con los registros de muestreos físicos
    con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql        = create_engine(con_engine)
    tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
    conn_psql.dispose()

    datos['id_salida']  = numpy.zeros(datos.shape[0],dtype=int)
 
    # PELACUS
    if id_programa == 1:        

        datos['año_salida']  = numpy.zeros(datos.shape[0],dtype=int)

        for idato in range(datos.shape[0]):
            datos['año_salida'][idato] = datos['fecha_muestreo'][idato].year
            
        anhos_salidas_mar = datos['año_salida'].unique()
        fecha_salida      = [None]*len(anhos_salidas_mar)
        fecha_llegada     = [None]*len(anhos_salidas_mar)
        for ianho in range(len(anhos_salidas_mar)):
            
            subset         = datos[datos['año_salida']==anhos_salidas_mar[ianho]]
            fechas_anuales = subset['fecha_muestreo'].unique()
            fecha_salida[ianho] = min(fechas_anuales)
            fecha_llegada[ianho] = max(fechas_anuales)
            
            identificador_estaciones_muestreadas = list(subset['id_estacion_temp'].unique())
            estaciones_muestreadas               =[None]*len(identificador_estaciones_muestreadas)
            for iestacion in range(len(estaciones_muestreadas)):
                estaciones_muestreadas[iestacion] = tabla_estaciones['nombre_estacion'][tabla_estaciones['id_estacion']==identificador_estaciones_muestreadas[iestacion]].iloc[0]
            json_estaciones        = json.dumps(estaciones_muestreadas)
        


            instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
            VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO UPDATE SET (nombre_salida,nombre_programa,tipo_salida,fecha_retorno,estaciones) = ROW(EXCLUDED.nombre_salida,EXCLUDED.nombre_programa,EXCLUDED.tipo_salida,EXCLUDED.fecha_retorno,EXCLUDED.estaciones);''' 
                
            conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
            cursor = conn.cursor()

            nombre_salida = nombre_programa + ' ' + str(anhos_salidas_mar[ianho]) 
          
            cursor.execute(instruccion_sql, (nombre_salida,int(id_programa),nombre_programa,tipo_salida,fecha_salida[ianho],fecha_llegada[ianho],json_estaciones))
            conn.commit()
            
            # Recupera el identificador de la salida al mar
            instruccion_sql = "SELECT id_salida FROM salidas_muestreos WHERE nombre_salida = '" + nombre_salida + "';"
            cursor.execute(instruccion_sql)
            id_salida =cursor.fetchone()[0]
            conn.commit()

            datos['id_salida'][datos['año_salida']==anhos_salidas_mar[ianho]] = id_salida
            
            cursor.close()
            conn.close()

        datos = datos.drop(columns=['año_salida']) 

    # RADIALES
    if id_programa == 3:
    
        # Encuentra las fechas de muestreo únicas
        fechas_salidas_mar = datos['fecha_muestreo'].unique()
        
        # Asigna nombre a la salida
        meses = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']
           
        # Listado con los identificadores de buque en función de la configuración del perfilador
        id_buque              = [1,2,3,2]
        
        instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,buque,estaciones)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO UPDATE SET (nombre_salida,nombre_programa,tipo_salida,fecha_retorno,buque,estaciones) = ROW(EXCLUDED.nombre_salida,EXCLUDED.nombre_programa,EXCLUDED.tipo_salida,EXCLUDED.fecha_retorno,EXCLUDED.buque,EXCLUDED.estaciones);''' 
                
        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
        cursor = conn.cursor()
        for isalida in range(len(fechas_salidas_mar)):
            
            # Encuentra las estaciones muestreadas
            subset_salida                        = datos[datos['fecha_muestreo']==fechas_salidas_mar[isalida]]
            identificador_estaciones_muestreadas = list(subset_salida['id_estacion_temp'].unique())
            estaciones_muestreadas               =[None]*len(identificador_estaciones_muestreadas)
            for iestacion in range(len(estaciones_muestreadas)):
                estaciones_muestreadas[iestacion] = tabla_estaciones['nombre_estacion'][tabla_estaciones['id_estacion']==identificador_estaciones_muestreadas[iestacion]].iloc[0]
            json_estaciones        = json.dumps(estaciones_muestreadas)
            
            id_configuracion_perfilador = list(subset_salida['configuracion_perfilador'].unique())[0]
            buque                       = id_buque[id_configuracion_perfilador-1]
          
            nombre_salida = nombre_programa + ' ' + tipo_salida + ' ' +   str(meses[fechas_salidas_mar[isalida].month-1]) + ' ' +  str(fechas_salidas_mar[isalida].year)
          
            cursor.execute(instruccion_sql, (nombre_salida,int(id_programa),nombre_programa,tipo_salida,fechas_salidas_mar[isalida],fechas_salidas_mar[isalida],int(buque),json_estaciones))
            conn.commit()
            
            # Recupera el identificador de la salida al mar
            instruccion_sql_recupera = "SELECT id_salida FROM salidas_muestreos WHERE nombre_salida = '" + nombre_salida + "';"
            cursor.execute(instruccion_sql_recupera)
            id_salida =cursor.fetchone()[0]
            conn.commit()
    
            datos['id_salida'][datos['fecha_muestreo']==fechas_salidas_mar[isalida]] = id_salida
            
        cursor.close()
        conn.close()
        
    # RADPROF
    if id_programa == 5:
        
        fecha_salida  = min(datos['fecha_muestreo'])
        fecha_llegada = max(datos['fecha_muestreo'])
        
        identificador_estaciones_muestreadas = list(datos['id_estacion_temp'].unique())
        estaciones_muestreadas               =[None]*len(identificador_estaciones_muestreadas)
        for iestacion in range(len(estaciones_muestreadas)):
            estaciones_muestreadas[iestacion] = tabla_estaciones['nombre_estacion'][tabla_estaciones['id_estacion']==identificador_estaciones_muestreadas[iestacion]].iloc[0]
        json_estaciones        = json.dumps(estaciones_muestreadas)
    
    
        instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
        VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO UPDATE SET (nombre_salida,nombre_programa,tipo_salida,fecha_retorno,estaciones) = ROW(EXCLUDED.nombre_salida,EXCLUDED.nombre_programa,EXCLUDED.tipo_salida,EXCLUDED.fecha_retorno,EXCLUDED.estaciones);''' 
    
        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
        cursor = conn.cursor()
    
        nombre_salida = nombre_programa + ' ' + str(fecha_salida.year) 
      
        cursor.execute(instruccion_sql, (nombre_salida,int(id_programa),nombre_programa,tipo_salida,fecha_salida,fecha_llegada,json_estaciones))
        conn.commit()
        
        # Recupera el identificador de la salida al mar
        instruccion_sql = "SELECT id_salida FROM salidas_muestreos WHERE nombre_salida = '" + nombre_salida + "';"
        cursor.execute(instruccion_sql)
        id_salida =cursor.fetchone()[0]
        conn.commit()
    
        datos['id_salida'] = id_salida
        
        cursor.close()
        conn.close()
                
    

    return datos




###########################################################################
######## FUNCION PARA ENCONTRAR EL IDENTIFICADOR DE CADA REGISTRO  ########
###########################################################################

def evalua_registros(datos,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto):
    
    # Recupera la tabla con los registros de los muestreos
    con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql        = create_engine(con_engine)
    tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
    tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
    tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn_psql)
       
    datos['id_muestreo_temp']  = numpy.zeros(datos.shape[0],dtype=int)
    
    # si no hay ningun valor en la tabla de registro, meter directamente todos los datos registrados
    if tabla_muestreos.shape[0] == 0:
    
        # genera un dataframe con las variables que interesa introducir en la base de datos
        exporta_registros                    = datos[['id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','configuracion_perfilador','configuracion_superficie']]
        # añade el indice de cada registro
        indices_registros                    = numpy.arange(1,(exporta_registros.shape[0]+1))    
        exporta_registros['id_muestreo']     = indices_registros
        # renombra la columna con información de la estación muestreada
        exporta_registros                    = exporta_registros.rename(columns={"id_estacion_temp":"estacion",'id_salida':'salida_mar'})
        # # añade el nombre del muestreo
        exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
        for idato in range(exporta_registros.shape[0]):    
            nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'][idato]]['nombre_estacion'].iloc[0]
            
            nombre_muestreo     = abreviatura_programa + '_' + datos['fecha_muestreo'][idato].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
            if datos['num_cast'][idato] is not None:
                nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'][idato]))
            if datos['botella'][idato] is not None:
                nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'][idato]))                
                
            exporta_registros['nombre_muestreo'][idato]  = nombre_muestreo

            # if datos['prof_referencia'][idato] is not None:
            #     str_profundidad = str(round(datos['prof_referencia'][idato]))
            # else:
            #     str_profundidad = str(round(datos['presion_ctd'][idato])) 
            #exporta_registros['nombre_muestreo'][idato]  = tabla_salidas['nombre_salida'][tabla_salidas['id_salida']==exporta_registros['salida_mar'][idato]].iloc[0] + ' E' + str(nombre_estacion) + ' P' + str_profundidad            
            
            datos['id_muestreo_temp'] [idato]            = idato + 1
            
            
        # Inserta en base de datos        
        exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
        exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append') 
    
    # En caso contrario hay que ver registro a registro, si ya está incluido en la base de datos
    else:
    
        ultimo_registro_bd         = max(tabla_muestreos['id_muestreo'])
        datos['io_nuevo_muestreo'] = numpy.ones(datos.shape[0],dtype=int)

        for idato in range(datos.shape[0]):

            for idato_existente in range(tabla_muestreos.shape[0]):
                
                # Registro ya incluido, recuperar el identificador
                if tabla_muestreos['estacion'][idato_existente] == datos['id_estacion_temp'][idato] and tabla_muestreos['fecha_muestreo'][idato_existente] == datos['fecha_muestreo'][idato] and  tabla_muestreos['hora_muestreo'][idato_existente] == datos['hora_muestreo'][idato] and  tabla_muestreos['presion_ctd'][idato_existente] == datos['presion_ctd'][idato] and  tabla_muestreos['configuracion_perfilador'][idato_existente] == datos['configuracion_perfilador'][idato] and  tabla_muestreos['configuracion_superficie'][idato_existente] == datos['configuracion_superficie'][idato]:
                    datos['id_muestreo_temp'] [idato] =  tabla_muestreos['id_muestreo'][idato_existente]    
                    datos['io_nuevo_muestreo'][idato] = 0
            
            # Nuevo registro
            if datos['io_nuevo_muestreo'][idato] == 1:
                # Asigna el identificador (siguiente al máximo disponible)
                ultimo_registro_bd                = ultimo_registro_bd + 1
                datos['id_muestreo_temp'][idato]  = ultimo_registro_bd  
               
        
        if numpy.count_nonzero(datos['io_nuevo_muestreo']) > 0:
        
            # Genera un dataframe sólo con los valores nuevos, a incluir (io_nuevo_muestreo = 1)
            nuevos_muestreos  = datos[datos['io_nuevo_muestreo']==1]
            # Mantén sólo las columnas que interesan
            exporta_registros = nuevos_muestreos[['id_muestreo_temp','id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','configuracion_perfilador','configuracion_superficie']]
                        
            # Cambia el nombre de la columna de estaciones
            exporta_registros = exporta_registros.rename(columns={"id_estacion_temp":"estacion","id_muestreo_temp":"id_muestreo",'id_salida':'salida_mar'})
            # Indice temporal
            exporta_registros['indice_temporal'] = numpy.arange(0,exporta_registros.shape[0])
            exporta_registros.set_index('indice_temporal',drop=True,append=False,inplace=True)
            # Añade el nombre del muestreo
            exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
            for idato in range(exporta_registros.shape[0]):    
                nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'][idato]]['nombre_estacion'].iloc[0]
              
                nombre_muestreo     = abreviatura_programa + '_' + datos['fecha_muestreo'][idato].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
                if datos['num_cast'][idato] is not None:
                    nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'][idato]))
                if datos['botella'][idato] is not None:
                    nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'][idato]))                
                 
                exporta_registros['nombre_muestreo'][idato]  = nombre_muestreo
                
                
                # if datos['prof_referencia'][idato] is not None:
                #     str_profundidad = str(round(datos['prof_referencia'][idato]))
                # else:
                #     str_profundidad = str(round(datos['presion_ctd'][idato]))  
                #exporta_registros['nombre_muestreo'][idato]  = tabla_salidas['nombre_salida'][tabla_salidas['id_salida']==exporta_registros['salida_mar'][idato]].iloc[0] + ' ' + str(nombre_estacion) + ' P' + str_profundidad            


            # # Inserta el dataframe resultante en la base de datos 
            exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
            exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append')    
    
    conn_psql.dispose() # Cierra la conexión con la base de datos 
    
    return datos







################################################################################
######## FUNCION PARA INSERTAR LOS DATOS DE FISICA EN LA BASE DE DATOS  ########
################################################################################

def inserta_datos_fisica(datos,direccion_host,base_datos,usuario,contrasena,puerto):
  
    # Recupera la tabla con los registros de muestreos físicos
    con_engine                = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql                 = create_engine(con_engine)
    tabla_registros_fisica    = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn_psql)
    
    # Genera un dataframe solo con las variales fisicas de los datos a importar 
    datos_fisica = datos[['temperatura_ctd', 'temperatura_ctd_qf','salinidad_ctd','salinidad_ctd_qf','par_ctd','par_ctd_qf','turbidez_ctd','turbidez_ctd_qf','id_muestreo_temp']]
    datos_fisica = datos_fisica.rename(columns={"id_muestreo_temp": "muestreo"})
    
    # # Si no existe ningún registro en la base de datos, introducir todos los datos disponibles
    if tabla_registros_fisica.shape[0] == 0:
        datos_fisica.set_index('muestreo',drop=True,append=False,inplace=True)
        datos_fisica.to_sql('datos_discretos_fisica', conn_psql,if_exists='append')
        
    
    # En caso contrario, comprobar qué parte de la información está en la base de datos
    else: 
        # Elimina, en el dataframe con los datos de la base de datos, los registros que ya están en los datos a importar
        for idato in range(datos_fisica.shape[0]):
            try:
                tabla_registros_fisica = tabla_registros_fisica.drop(tabla_registros_fisica[tabla_registros_fisica.muestreo == datos_fisica['muestreo'][idato]].index)
            except:
                pass
            
        # Une ambos dataframes, el que contiene los datos nuevo y el que tiene los datos que ya están en la base de datos
        datos_conjuntos = pandas.concat([tabla_registros_fisica, datos_fisica])
            
        vector_identificadores            = numpy.arange(1,datos_conjuntos.shape[0]+1)    
        datos_conjuntos['id_disc_fisica'] = vector_identificadores
        
        datos_conjuntos.set_index('id_disc_fisica',drop=True,append=False,inplace=True)
        
        # borra los registros existentes en la tabla (no la tabla en sí, para no perder tipos de datos y referencias)
        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
        cursor = conn.cursor()
        instruccion_sql = "TRUNCATE datos_discretos_fisica;"
        cursor.execute(instruccion_sql)
        conn.commit()
        cursor.close()
        conn.close() 
        
        # Inserta el dataframe resultante en la base de datos 
        datos_conjuntos.to_sql('datos_discretos_fisica', conn_psql,if_exists='append')
    

    conn_psql.dispose() # Cierra la conexión con la base de datos 


#######################################################################################
######## FUNCION PARA INSERTAR LOS DATOS DE BIOGEOQUIMICA EN LA BASE DE DATOS  ########
#######################################################################################


def inserta_datos_biogeoquimica(datos,direccion_host,base_datos,usuario,contrasena,puerto):
  
    # Recupera la tabla con los registros de muestreos físicos
    con_engine                = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql                 = create_engine(con_engine)
    tabla_registros_biogoquim = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn_psql)
    
    # Genera un dataframe solo con las variales biogeoquimicas de los datos a importar 
    datos_biogeoquimica = datos[['id_muestreo_temp','fluorescencia_ctd','fluorescencia_ctd_qf','oxigeno_ctd','oxigeno_ctd_qf','oxigeno_wk','oxigeno_wk_qf',
                                 'nitrogeno_total','nitrogeno_total_qf','nitrato','nitrato_qf','nitrito','nitrito_qf','amonio','amonio_qf','fosfato','fosfato_qf','silicato','silicato_qf','tcarbn','tcarbn_qf','doc','doc_qf',
                                 'cdom','cdom_qf','clorofila_a','clorofila_a_qf','alcalinidad','alcalinidad_qf','ph','ph_qf','ph_metodo','r_clor','r_clor_qf','r_per','r_per_qf','co3_temp']]    
    datos_biogeoquimica = datos_biogeoquimica.rename(columns={"id_muestreo_temp": "muestreo"})
    
    # Elimina, en el dataframe con los datos de la base de datos, los registros que ya están en los datos a importar
    for idato in range(tabla_registros_biogoquim.shape[0]):
        try:
            tabla_registros_biogoquim = tabla_registros_biogoquim.drop(tabla_registros_biogoquim[tabla_registros_biogoquim.muestreo == datos_biogeoquimica['muestreo'][idato]].index)
        except:
            pass
        
    # Une ambos dataframes, el que contiene los datos nuevo y el que tiene los datos que ya están en la base de datos
    datos_conjuntos = pandas.concat([tabla_registros_biogoquim, datos_biogeoquimica])
        
    vector_identificadores            = numpy.arange(1,datos_conjuntos.shape[0]+1)    
    datos_conjuntos['id_disc_biogeoquim'] = vector_identificadores
    
    datos_conjuntos.set_index('id_disc_biogeoquim',drop=True,append=False,inplace=True)
    
    # borra los registros existentes en la tabla (no la tabla en sí, para no perder tipos de datos y referencias)
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()
    instruccion_sql = "TRUNCATE datos_discretos_biogeoquimica;"
    cursor.execute(instruccion_sql)
    conn.commit()
    cursor.close()
    conn.close() 
    
    # Inserta el dataframe resultante en la base de datos 
    datos_conjuntos.to_sql('datos_discretos_biogeoquimica', conn_psql,if_exists='append')

    conn_psql.dispose() # Cierra la conexión con la base de datos 
    





    
    
 
###################################################################################################
######## FUNCION PARA ACTUALIZAR LA TABLA DE ESTADOS, UTILIZADA POR LA APLICACIO STREAMLIT ########
###################################################################################################
    
def actualiza_estado(datos,fecha_actualizacion,id_programa,nombre_programa,itipo_informacion,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto):


    
    # Busca de cuántos años diferentes contiene información el dataframe
    vector_auxiliar_tiempo = numpy.zeros(datos.shape[0],dtype=int)
    for idato in range(datos.shape[0]):
        vector_auxiliar_tiempo[idato] = datos['fecha_muestreo'][idato].year
    anhos_muestreados                 = numpy.unique(vector_auxiliar_tiempo)
    datos['año']                      = vector_auxiliar_tiempo 
    
    # Procesado para cada uno de los años incluidos en el dataframe importado
    for ianho in range(len(anhos_muestreados)):
        
        anho_procesado = anhos_muestreados[ianho]
               
        # Selecciona la información de cada uno de los años 
        fechas_anuales  = datos['fecha_muestreo'][datos['año']==anhos_muestreados[ianho]]
        
        # Encuentra la fecha de final de muestreo
        fecha_final_muestreo = fechas_anuales.max()
        
        # Recupera de la base de datos las fechas de análisis y procesado (si están disponibles)
        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
        cursor = conn.cursor()    
        
        instruccion_sql = 'SELECT programa,nombre_programa,año,fecha_final_muestreo,fecha_analisis_laboratorio,fecha_post_procesado,contacto_muestreo,contacto_post_procesado FROM estado_procesos WHERE programa = ' + str(id_programa) +' AND año = ' + str(anho_procesado) +';'
        cursor.execute(instruccion_sql)
        datos_bd =cursor.fetchall()
        conn.commit()
        
        # Si no hay datos, genera un vector de valores nulos
        if len(datos_bd) == 0:
                
            # Genera el vector con los datos a insertar. diferente según sea análisis o post-procesado
            if itipo_informacion == 1: # La información a insertar es un nuevo registro de análisis de laboratorio
                datos_insercion     = [int(id_programa),nombre_programa,int(anho_procesado),fecha_final_muestreo,fecha_actualizacion,None,None,email_contacto]
            
            if itipo_informacion == 2: # La información a insertar es un registro de post-procesado 
                datos_insercion     = [int(id_programa),nombre_programa,int(anho_procesado),fecha_final_muestreo,None,fecha_actualizacion,None,email_contacto]
            
            if itipo_informacion == 3: # La información a insertar es un registro de entrada de datos 
                datos_insercion     = [int(id_programa),nombre_programa,int(anho_procesado),fecha_final_muestreo,None,None,email_contacto,None]
            
            
            # Inserta la información en la base de datos
            instruccion_sql = "INSERT INTO estado_procesos (programa,nombre_programa,año,fecha_final_muestreo,fecha_analisis_laboratorio,fecha_post_procesado,contacto_muestreo,contacto_post_procesado) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,año) DO UPDATE SET (nombre_programa,fecha_final_muestreo,fecha_analisis_laboratorio,fecha_post_procesado,contacto_muestreo,contacto_post_procesado) = (EXCLUDED.nombre_programa,EXCLUDED.fecha_final_muestreo,EXCLUDED.fecha_analisis_laboratorio,EXCLUDED.fecha_post_procesado,EXCLUDED.contacto_muestreo,EXCLUDED.contacto_post_procesado);"   
            cursor.execute(instruccion_sql, (datos_insercion))
            conn.commit()

        # Si la base de datos ya contiene registros del programa y año a insertar, actualizar las fechas correspondientes
        else:
            if itipo_informacion == 1:
                instruccion_sql = "UPDATE estado_procesos SET fecha_analisis_laboratorio = %s,contacto_post_procesado = %s WHERE programa = %s AND año = %s;"   
                cursor.execute(instruccion_sql, (fecha_actualizacion,email_contacto,int(id_programa),int(anho_procesado)))
                conn.commit()                

            if itipo_informacion == 2:
                instruccion_sql = "UPDATE estado_procesos SET fecha_post_procesado = %s,contacto_post_procesado = %s WHERE programa = %s AND año = %s;"   
                cursor.execute(instruccion_sql, (fecha_actualizacion,email_contacto,int(id_programa),int(anho_procesado)))
                conn.commit()  
                
            if itipo_informacion == 3:
                instruccion_sql = "UPDATE estado_procesos SET fecha_entrada_datos = %s,contacto_post_procesado = %s WHERE programa = %s AND año = %s;"   
                cursor.execute(instruccion_sql, (fecha_actualizacion,email_contacto,int(id_programa),int(anho_procesado)))
                conn.commit() 
    
    cursor.close()
    conn.close()    
    
    






# ####################################################################################
# ######## FUNCION PARA RECUPERAR EL IDENTIFICADOR DE UN PROGRAMA DETERMINADO ########
# ####################################################################################
def recupera_id_programa(nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto):
    
    # recupera el identificador del programa
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()
    
    # Identificador del programa 
    instruccion_sql = "SELECT id_programa FROM programas WHERE nombre_programa = '" + nombre_programa + "';"
    cursor.execute(instruccion_sql)
    id_programa =cursor.fetchone()[0]
    conn.commit()

    # Abrevatura del programa 
    instruccion_sql = "SELECT abreviatura FROM programas WHERE nombre_programa = '" + nombre_programa + "';"
    cursor.execute(instruccion_sql)
    abreviatura_programa =cursor.fetchone()[0]
    conn.commit()    
    
    cursor.close()
    conn.close()    

    return id_programa,abreviatura_programa








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
    posicion_separador = nombre_archivo.index('+')
    nombre_estacion    = nombre_archivo[8:posicion_separador].upper() + 'CO'                
    id_estacion        = df_estaciones_radiales['id_estacion'][df_estaciones_radiales['nombre_estacion']==nombre_estacion].iloc[0] 
    
    # Identifica la fecha del muestreo
    fecha_salida_texto = nombre_archivo[0:8]
    fecha_salida       = datetime.datetime.strptime(fecha_salida_texto, '%Y%m%d').date()
    
    
    id_estacion              = tabla_estaciones['id_estacion'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]
    profundidades_referencia = tabla_estaciones['profundidades_referencia'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]
    lat_estacion             = tabla_estaciones['latitud'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]
    lon_estacion             = tabla_estaciones['longitud'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]
    
    
    # Genera las listas en las que se guardarán los datos si éstos existen
    datos_botella     = []
    datos_salinidad   = []
    datos_temperatura = []
    datos_presion     = []
    datos_PAR         = []
    datos_fluor       = []
    datos_O2          = []
    
    # Lee el archivo .btl y escribe la información de las botellas en un archivo temporal
    cast_muestreo          = 1 # Asinga este valor por si no se introdujo ningún dato en el muestreo
    fecha_muestreo_archivo = None
    for ilinea in range(len(datos_archivo)):
        texto_linea = datos_archivo[ilinea]
        if texto_linea[0:1] == '#' or texto_linea[0:1] == '*':
            # if texto_linea[0:8] == '** Time:': # Línea con hora del cast
            #     hora_muestreo = datetime.datetime.strptime(texto_linea[8:13],'%H:%M').time() 
                
            if texto_linea[0:14] == '* System UTC =': # Línea con hora del cast
                hora_muestreo = datetime.datetime.strptime(texto_linea[27:35],'%H:%M:%S').time() 

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
                                    
    
    
    
    # Comprueba que la fecha contenida en el archivo y la del nombre del archivo son la misma
    if fecha_muestreo_archivo is not None and fecha_muestreo_archivo != fecha_salida:
    
        mensaje_error  = 'La fecha indicada en el nombre del archivo no coincide con la que figura en dentro del mismo'
        datos_botellas = None
        io_par         = 0
        io_fluor       = 0
        io_O2          = 0
    
    else:
        
        # Une las listas en un dataframe
        datos_botellas = pandas.DataFrame(list(zip(datos_botella, datos_salinidad,datos_temperatura,datos_presion)),
                       columns =['botella', 'salinidad_ctd','temperatura_ctd','presion_ctd'])
        
        # Añade columnas con el QF de T y S
        datos_botellas['temperatura_ctd_qf'] = 2
        datos_botellas['salinidad_ctd_qf']   = 2
        
        if io_par == 1:
            datos_botellas['par_ctd']              =  datos_PAR
            datos_botellas['par_ctd_qf']           = 2
        if io_fluor == 1:
            datos_botellas['fluorescencia_ctd']    =  datos_fluor
            datos_botellas['fluorescencia_ctd_qf'] = 2
        if io_O2 == 1:
            datos_botellas['oxigeno_ctd']          =  datos_O2
            datos_botellas['oxigeno_ctd_qf']       = 2
            
            
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
        datos_botellas['latitud']                  = lat_estacion  
        datos_botellas['longitud']                 = lon_estacion
        datos_botellas['fecha_muestreo']           = fecha_salida
        datos_botellas,textos_aviso                = control_calidad(datos_botellas,direccion_host,base_datos,usuario,contrasena,puerto)            
        
        # Añade columnas con datos del muestreo 
        datos_botellas['id_estacion_temp']         = numpy.zeros(datos_botellas.shape[0],dtype=int)
        datos_botellas['id_estacion_temp']         = id_estacion
        datos_botellas['estacion']                 = id_estacion
        datos_botellas['fecha_muestreo']           = fecha_salida
        datos_botellas['hora_muestreo']            = hora_muestreo
        datos_botellas['num_cast']                 = cast_muestreo
        datos_botellas['configuracion_perfilador'] = 1
        datos_botellas['configuracion_superficie'] = 1
        datos_botellas['programa']                 = id_programa_elegido
        
        mensaje_error = []
        
    return mensaje_error,datos_botellas,io_par,io_fluor,io_O2


######################################################################
######## FUNCION PARA LEER DATOS DE BOTELLAs (ARCHIVOS .BTL)  ########
######################################################################
def control_calidad_nutrientes(datos_procesados,listado_variables,direccion_host,base_datos,usuario,contrasena,puerto):

    import streamlit as st
    import matplotlib.pyplot as plt
    from FUNCIONES.FUNCIONES_AUXILIARES import init_connection    

    # Recupera los datos disponibles en la base de datos
    conn                      = init_connection()
    df_muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
    df_datos_biogeoquimicos   = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn)
    df_indices_calidad        = psql.read_sql('SELECT * FROM indices_calidad', conn)
    df_salidas                = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
    df_estaciones             = psql.read_sql('SELECT * FROM estaciones', conn)
    df_programas              = psql.read_sql('SELECT * FROM programas', conn)
    conn.close()

    id_dato_malo              = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Malo'].iloc[0]

    
    ### CONTROL DE CALIDAD DE LOS DATOS

    # Despliega menús de selección de la variable, salida y la estación a controlar                
    col1, col2 = st.columns(2,gap="small")
    with col1: 
        
        listado_programas         = datos_procesados['programa'].unique()
        df_programas_muestreados  = df_programas[df_programas['id_programa'].isin(listado_programas)]
        programa_seleccionado     = st.selectbox('Programa',(df_programas_muestreados['nombre_programa']))
        indice_programa           = df_programas['id_programa'][df_programas['nombre_programa']==programa_seleccionado].iloc[0]

    with col2:
        
        df_prog_sel               = datos_procesados[datos_procesados['programa']==indice_programa]
        anhos_disponibles         = df_prog_sel['año'].unique()
        anho_seleccionado         = st.selectbox('Año',(anhos_disponibles))
        
        df_prog_anho_sel          = df_prog_sel[df_prog_sel['año']==anho_seleccionado]


    col1, col2 = st.columns(2,gap="small")
    with col1: 

        
        listado_salidas           = df_prog_anho_sel['salida_mar'].unique()
        df_salidas_muestreadas    = df_salidas[df_salidas['id_salida'].isin(listado_salidas)]
        salida_seleccionada       = st.selectbox('Salida',(df_salidas_muestreadas['nombre_salida']))
        indice_salida             = df_salidas['id_salida'][df_salidas['nombre_salida']==salida_seleccionada].iloc[0]

        df_prog_anho_sal_sel      = df_prog_anho_sel[df_prog_anho_sel['salida_mar']==indice_salida]

    with col2:

      
        listado_id_estaciones        = df_prog_anho_sal_sel['estacion'].unique() 
        df_estaciones_disponibles    = df_estaciones[df_estaciones['id_estacion'].isin(listado_id_estaciones)]

        estacion_seleccionada        = st.selectbox('Estación',(df_estaciones_disponibles['nombre_estacion']))
        indice_estacion              = df_estaciones_disponibles['id_estacion'][df_estaciones_disponibles['nombre_estacion']==estacion_seleccionada].iloc[0]
        
        df_prog_anho_sal_est_sel     = df_prog_anho_sal_sel[df_prog_anho_sal_sel['estacion']==indice_salida]
        
    col1, col2,col3 = st.columns(3,gap="small")
    with col1: 
        
        listado_casts_estaciones  = df_prog_anho_sal_est_sel['num_cast'].unique() 
        cast_seleccionado         = st.selectbox('Cast',(listado_casts_estaciones))
        
    with col2: 
        variable_seleccionada     = st.selectbox('Variable',(listado_variables))
        indice_variable           = listado_variables.index(variable_seleccionada)
    with col3:  
        meses_offset              = st.number_input('Intervalo meses:',value=1)
    
    # Selecciona los datos correspondientes a la estación y salida seleccionada
    df_seleccion               = datos_procesados[(datos_procesados["programa"] == indice_programa) & (datos_procesados["año"] == anho_seleccionado) & (datos_procesados["estacion"] == indice_estacion) & (datos_procesados["salida_mar"] == indice_salida) & (datos_procesados["num_cast"] == cast_seleccionado)]
    

    # Recupera los datos disponibles de la misma estación, para la misma variable
    listado_muestreos_estacion = df_muestreos['id_muestreo'][df_muestreos['estacion']==indice_estacion]
    df_disponible_bd           = df_datos_biogeoquimicos[df_datos_biogeoquimicos['muestreo'].isin(listado_muestreos_estacion)]
    
    df_disponible_bd            = df_disponible_bd.rename(columns={"muestreo": "id_muestreo"}) # Para igualar los nombres de columnas                                               
    df_disponible_bd            = pandas.merge(df_muestreos, df_disponible_bd, on="id_muestreo")
   
    # Determina los meses que marcan el rango de busqueda
    df_seleccion    = df_seleccion.sort_values('fecha_muestreo')
    st.text(df_seleccion)
    # fecha_minima    = df_seleccion['fecha_muestreo'].iloc[0][0] - datetime.timedelta(days=meses_offset*30)
    # fecha_maxima    = df_seleccion['fecha_muestreo'].iloc[-1][0] + datetime.timedelta(days=meses_offset*30)  
    fecha_minima    = df_seleccion['fecha_muestreo'].iloc[0] - datetime.timedelta(days=meses_offset*30)
    fecha_maxima    = df_seleccion['fecha_muestreo'].iloc[-1] + datetime.timedelta(days=meses_offset*30)  


    if fecha_minima.year < fecha_maxima.year:
        listado_meses_1 = numpy.arange(fecha_minima.month,13)
        listado_meses_2 = numpy.arange(1,fecha_maxima.month+1)
        listado_meses   = numpy.concatenate((listado_meses_1,listado_meses_2))
    
    else:
        listado_meses   = numpy.arange(fecha_minima.month,fecha_maxima.month+1)
 
    listado_meses = listado_meses.tolist()
   
    
    # Busca los datos de la base de datos dentro del rango de meses seleccionados
    df_disponible_bd['io_fecha'] = numpy.zeros(df_disponible_bd.shape[0],dtype=int)
    for idato in range(df_disponible_bd.shape[0]):
        if (df_disponible_bd['fecha_muestreo'].iloc[idato]).month in listado_meses:
            df_disponible_bd['io_fecha'].iloc[idato] = 1
            
    df_rango_temporal = df_disponible_bd[df_disponible_bd['io_fecha']==1]

    ################# GRAFICOS ################

    # Representa un gráfico con la variable seleccionada junto a los oxígenos
    if df_seleccion[variable_seleccionada].isnull().all():
        io_control = 0
        texto_error = "La base de datos no contiene información para la variable, salida y estación seleccionadas"
        st.warning(texto_error, icon="⚠️")
        
    else:
        io_control = 1
    
        fig, (ax, az) = plt.subplots(1, 2, gridspec_kw = {'wspace':0.05, 'hspace':0}, width_ratios=[3, 1])
       
        ax.plot(df_disponible_bd[listado_variables[indice_variable]],df_disponible_bd['presion_ctd'],'.',color='#C0C0C0',label='DATO PREVIO(OK)')
        ax.plot(df_rango_temporal[listado_variables[indice_variable]],df_rango_temporal['presion_ctd'],'.',color='#404040',label='DATO PREVIO(INTERVALO)')
           
        ax.plot(df_seleccion[variable_seleccionada],df_seleccion['presion_ctd'],'.r',label='PROCESADO' )
        texto_eje = variable_seleccionada + '(\u03BCmol/kg)'
        ax.set(xlabel=texto_eje)
        ax.set(ylabel='Presion (db)')
        ax.invert_yaxis()
        rango_profs = ax.get_ylim()
        # Añade el nombre de cada punto
        nombre_muestreos = [None]*df_seleccion.shape[0]
        for ipunto in range(df_seleccion.shape[0]):
            if df_seleccion['botella'].iloc[ipunto] is None:
                nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
            else:
                nombre_muestreos[ipunto] = 'Bot.' + str(df_seleccion['botella'].iloc[ipunto])
            ax.annotate(nombre_muestreos[ipunto], (df_seleccion[variable_seleccionada].iloc[ipunto], df_seleccion['presion_ctd'].iloc[ipunto]))
       
        qf_variable_seleccionada = listado_variables[indice_variable] + '_qf'
        datos_malos = df_disponible_bd[df_disponible_bd[qf_variable_seleccionada]==id_dato_malo]
        ax.plot(datos_malos[listado_variables[indice_variable]],datos_malos['presion_ctd'],'.',color='#00CCCC',label='DATO PREVIO(MALO)')    
        ax.legend(loc='upper center',bbox_to_anchor=(0.5, 1.15),ncol=2, fancybox=True,fontsize=7)
        
        io_plot = 0
        if not df_seleccion['oxigeno_ctd'].isnull().all(): 
            az.plot(df_seleccion['oxigeno_ctd'],df_seleccion['presion_ctd'],'.',color='#006633',label='OXIMETRO')
            io_plot = 1
                
        if not df_seleccion['oxigeno_wk'].isnull().all(): 
            az.plot(df_seleccion['oxigeno_wk'],df_seleccion['presion_ctd'],'.',color='#00CC66',label='WINKLER')
            io_plot = 1
            
        if io_plot == 1:
            az.set(xlabel='Oxigeno (\u03BCmol/kg)')
            az.yaxis.set_visible(False)
            az.invert_yaxis()
            az.set_ylim(rango_profs)
            az.legend(loc='upper center',bbox_to_anchor=(0.5, 1.15),ncol=1, fancybox=True,fontsize=7)
    
        st.pyplot(fig)
 
    if io_control == 1:    
        # Gráficos particulares para cada variable
        if variable_seleccionada == 'fosfato':
    
            fig, ax = plt.subplots()       
            ax.plot(df_disponible_bd['nitrato'],df_disponible_bd['fosfato'],'.',color='#C0C0C0')
            ax.plot(df_rango_temporal['nitrato'],df_rango_temporal['fosfato'],'.',color='#404040')
            ax.plot(df_seleccion['nitrato'],df_seleccion['fosfato'],'.r' )
            
            datos_malos = df_disponible_bd[df_disponible_bd['fosfato_qf']==id_dato_malo]
            ax.plot(datos_malos['nitrato'],datos_malos['fosfato'],'.',color='#00CCCC')
            datos_malos = df_disponible_bd[df_disponible_bd['nitrato_qf']==id_dato_malo]
            ax.plot(datos_malos['nitrato'],datos_malos['fosfato'],'.',color='#00CCCC')
            
            ax.set(xlabel='Nitrato (\u03BCmol/kg)')
            ax.set(ylabel='Fosfato (\u03BCmol/kg)')
    
            # Añade el nombre de cada punto
            nombre_muestreos = [None]*df_seleccion.shape[0]
            for ipunto in range(df_seleccion.shape[0]):
                if df_seleccion['botella'].iloc[ipunto] is None:
                    nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
                else:
                    nombre_muestreos[ipunto] = 'Bot.' + str(df_seleccion['botella'].iloc[ipunto])
                ax.annotate(nombre_muestreos[ipunto], (df_seleccion['nitrato'].iloc[ipunto], df_seleccion['fosfato'].iloc[ipunto]))
           
            st.pyplot(fig)
        
        elif variable_seleccionada == 'nitrato':
    
            if df_seleccion['ph'].isnull().all():         
                fig, ax = plt.subplots()      
            else:
                fig, (ax, az) = plt.subplots(1, 2, gridspec_kw = {'wspace':0.1, 'hspace':0}, width_ratios=[1, 1])      
    
            ax.plot(df_disponible_bd['nitrato'],df_disponible_bd['fosfato'],'.',color='#C0C0C0')
            ax.plot(df_rango_temporal['nitrato'],df_rango_temporal['fosfato'],'.',color='#404040')
            ax.plot(df_seleccion['nitrato'],df_seleccion['fosfato'],'.r' )
            
            datos_malos = df_disponible_bd[df_disponible_bd['fosfato_qf']==id_dato_malo]
            ax.plot(datos_malos['nitrato'],datos_malos['fosfato'],'.',color='#00CCCC')
            datos_malos = df_disponible_bd[df_disponible_bd['nitrato_qf']==id_dato_malo]
            ax.plot(datos_malos['nitrato'],datos_malos['fosfato'],'.',color='#00CCCC')
            
            ax.set(xlabel='Nitrato (\u03BCmol/kg)')
            ax.set(ylabel='Fosfato (\u03BCmol/kg)')
    
            # Añade el nombre de cada punto
            nombre_muestreos = [None]*df_seleccion.shape[0]
            for ipunto in range(df_seleccion.shape[0]):
                if df_seleccion['botella'].iloc[ipunto] is None:
                    nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
                else:
                    nombre_muestreos[ipunto] = 'Bot.' + str(df_seleccion['botella'].iloc[ipunto])
                ax.annotate(nombre_muestreos[ipunto], (df_seleccion['nitrato'].iloc[ipunto], df_seleccion['fosfato'].iloc[ipunto]))
    
    
            az.plot(df_disponible_bd['nitrato'],df_disponible_bd['ph'],'.',color='#C0C0C0')
            az.plot(df_rango_temporal['nitrato'],df_rango_temporal['ph'],'.',color='#404040')
            az.set(xlabel='Nitrato (\u03BCmol/kg)')
            az.set(ylabel='pH')
            if df_seleccion['ph'].isnull().all():    
                az.plot(df_seleccion['nitrato'],df_seleccion['ph'],'.r' )
                az.yaxis.tick_right()
                az.yaxis.set_label_position("right")
            
                # Añade el nombre de cada punto
                nombre_muestreos = [None]*df_seleccion.shape[0]
                for ipunto in range(df_seleccion.shape[0]):
                    if df_seleccion['botella'].iloc[ipunto] is None:
                        nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
                    else:
                        nombre_muestreos[ipunto] = 'Bot.' + str(df_seleccion['botella'].iloc[ipunto])
                    az.annotate(nombre_muestreos[ipunto], (df_seleccion['nitrato'].iloc[ipunto], df_seleccion['ph'].iloc[ipunto]))
         
    
            st.pyplot(fig)
      
        
        # Gráficos particulares para cada variable
        elif variable_seleccionada == 'silicato':
    
            if df_seleccion['silicato'].isnull().all() is False:         
    
                fig, ax = plt.subplots()       
                ax.plot(df_disponible_bd['silicato'],df_disponible_bd['alcalinidad'],'.',color='#C0C0C0')
                ax.plot(df_rango_temporal['silicato'],df_rango_temporal['alcalinidad'],'.',color='#404040')
                ax.plot(df_seleccion['silicato'],df_seleccion['alcalinidad'],'.r' )
                
                datos_malos = df_disponible_bd[df_disponible_bd['silicato_qf']==id_dato_malo]
                ax.plot(datos_malos['silicato'],datos_malos['alcalinidad'],'.',color='#00CCCC')
                datos_malos = df_disponible_bd[df_disponible_bd['alcalinidad_qf']==id_dato_malo]
                ax.plot(datos_malos['silicato'],datos_malos['alcalinidad_qf'],'.',color='#00CCCC')
                
                ax.set(xlabel='Silicato (\u03BCmol/kg)')
                ax.set(ylabel='Alcalinidad (\u03BCmol/kg)')
        
                # Añade el nombre de cada punto
                nombre_muestreos = [None]*df_seleccion.shape[0]
                for ipunto in range(df_seleccion.shape[0]):
                    if df_seleccion['botella'].iloc[ipunto] is None:
                        nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
                    else:
                        nombre_muestreos[ipunto] = 'Bot.' + str(df_seleccion['botella'].iloc[ipunto])
                    ax.annotate(nombre_muestreos[ipunto], (df_seleccion['silicato'].iloc[ipunto], df_seleccion['alcalinidad'].iloc[ipunto]))
               
                st.pyplot(fig)
        
        
        ################# FORMULARIOS CALIDAD ################        
    
        # Formulario para asignar banderas de calidad
        with st.form("Formulario", clear_on_submit=False):
                      
            indice_validacion = df_indices_calidad['indice'].tolist()
            texto_indice      = df_indices_calidad['descripcion'].tolist()
            qf_asignado       = numpy.zeros(df_seleccion.shape[0])
           
            for idato in range(df_seleccion.shape[0]):
               
                enunciado          = 'QF del muestreo ' + nombre_muestreos[idato]
                valor_asignado     = st.radio(enunciado,texto_indice,horizontal=True,key = idato,index = 1)
                qf_asignado[idato] = indice_validacion[texto_indice.index(valor_asignado)]
           
            io_envio = st.form_submit_button("Añadir resultados a la base de datos con los índices seleccionados")  
    
        if io_envio:
    
            with st.spinner('Actualizando la base de datos'):
           
                # Introducir los valores en la base de datos
                conn   = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                cursor = conn.cursor()  
       
                for idato in range(df_seleccion.shape[0]):
    
                    instruccion_sql = "UPDATE datos_discretos_biogeoquimica SET " + listado_variables[indice_variable] + ' = %s, ' + listado_variables[indice_variable] +  '_qf = %s WHERE id_disc_biogeoquim = %s;'
                    cursor.execute(instruccion_sql, (df_seleccion[variable_seleccionada].iloc[idato],int(qf_asignado[idato]),int(df_seleccion['id_disc_biogeoquim'].iloc[idato])))
                    conn.commit() 
    
                cursor.close()
                conn.close()   
    
            texto_exito = 'Datos de ' + variable_seleccionada + ' correspondientes a la salida ' + salida_seleccionada + ' añadidos correctamente'
            st.success(texto_exito)
       



################################################################
######## FUNCION PARA REALIZAR LA CORRECCIÓN DE DERIVA  ########
################################################################

def correccion_drift(datos_brutos,datos_corregidos,datos_estadillo,datos_referencias,variables_run,rendimiento_columna,temperatura_laboratorio):

    # Encuentra los índices (picos) correspondientes a la calbración
    indices_calibracion = numpy.asarray(datos_brutos['Peak Number'][datos_brutos['Cup Type']=='CALB']) - 1
       
    # Corrige las concentraciones a partir de los rendimientos de la coumna reductora
    datos_brutos['NO3_rendimiento'] = numpy.zeros(datos_brutos.shape[0])
    datos_brutos['TON_rendimiento'] = numpy.zeros(datos_brutos.shape[0])
    factor = ((datos_brutos['TON'].iloc[indices_calibracion[-1]]*rendimiento_columna/100) + datos_brutos['NO2'].iloc[indices_calibracion[-1]])/(datos_brutos['TON'].iloc[indices_calibracion[-1]] + datos_brutos['NO2'].iloc[indices_calibracion[-1]])
    for idato in range(datos_brutos.shape[0]):
        datos_brutos['NO3_rendimiento'].iloc[idato] = (datos_brutos['TON'].iloc[idato]*factor - datos_brutos['NO2'].iloc[idato])/(rendimiento_columna/100) 
        datos_brutos['TON_rendimiento'].iloc[idato] = datos_brutos['NO3_rendimiento'].iloc[idato] + datos_brutos['NO2'].iloc[idato]
    
    
    # Pasa las concentraciones a mol/kg
    datos_brutos['DENSIDAD'] = numpy.ones(datos_brutos.shape[0])
    for idato in range(datos_brutos.shape[0]):
        if datos_brutos['Sample ID'].iloc[idato] == 'RMN Low CE' :
            datos_brutos['DENSIDAD'].iloc[idato]  = (999.1+0.77*((datos_referencias['Sal'][0])-((temperatura_laboratorio-15)/5.13)-((temperatura_laboratorio-15)**2)/128))/1000
            
        elif datos_brutos['Sample ID'].iloc[idato] == 'RMN High CG':
            datos_brutos['DENSIDAD'].iloc[idato]  = (999.1+0.77*((datos_referencias['Sal'][1])-((temperatura_laboratorio-15)/5.13)-((temperatura_laboratorio-15)**2)/128))/1000
            
        else:
            for iestadillo in range(datos_estadillo.shape[0]):
                if datos_brutos['Sample ID'].iloc[idato] == datos_estadillo['ID'].iloc[iestadillo]:
                    datos_brutos['DENSIDAD'].iloc[idato] = (999.1+0.77*((datos_estadillo['SALCTD'].iloc[iestadillo])-((temperatura_laboratorio-15)/5.13)-((temperatura_laboratorio-15)**2)/128))/1000
                   
                    
    datos_brutos['TON_CONC'] = datos_brutos['TON_rendimiento']/datos_brutos['DENSIDAD']  
    datos_brutos['NO3_CONC'] = datos_brutos['NO3_rendimiento']/datos_brutos['DENSIDAD']  
    datos_brutos['NO2_CONC'] = datos_brutos['NO2']/datos_brutos['DENSIDAD']  
    datos_brutos['SiO2_CONC'] = datos_brutos['SiO2']/datos_brutos['DENSIDAD']  
    datos_brutos['PO4_CONC'] = datos_brutos['PO4']/datos_brutos['DENSIDAD']  
    
    
    ####  APLICA LA CORRECCIÓN DE DERIVA ####
    # Encuentra las posiciones de los RMNs
    posicion_RMN_bajos  = [i for i, e in enumerate(datos_brutos['Sample ID']) if e == 'RMN Low CE']
    posicion_RMN_altos  = [i for i, e in enumerate(datos_brutos['Sample ID']) if e == 'RMN High CG']
    
    for ivariable in range(len(variables_run)):
    #for ivariable in range(1):
        
        variable_concentracion  = variables_run[ivariable] + '_CONC'
        
        # Concentraciones de las referencias
        RMN_CE_variable = datos_referencias[variables_run[ivariable]].iloc[0]
        RMN_CI_variable = datos_referencias[variables_run[ivariable]].iloc[1]  
        
        # Concentraciones de las muestras analizadas como referencias
        RMN_altos       = datos_brutos[variable_concentracion][posicion_RMN_altos]
        RMN_bajos       = datos_brutos[variable_concentracion][posicion_RMN_bajos]
    
        # Predimensiona las rectas a y b
        posiciones_corr_drift = numpy.arange(posicion_RMN_altos[0]-1,posicion_RMN_bajos[1]+1)
        recta_at              = numpy.zeros(datos_brutos.shape[0])
        recta_bt              = numpy.zeros(datos_brutos.shape[0])
        
        store = numpy.zeros(datos_brutos.shape[0])
    
        pte_RMN      = (RMN_CI_variable-RMN_CE_variable)/(RMN_altos.iloc[0]-RMN_bajos.iloc[0]) 
        t_indep_RMN  = RMN_CE_variable- pte_RMN*RMN_bajos.iloc[0] 
    
        variable_drift = numpy.zeros(datos_brutos.shape[0])
    
        # Aplica la corrección basada de dos rectas, descrita en Hassenmueller
        for idato in range(posiciones_corr_drift[0],posiciones_corr_drift[-1]):
            factor_f        = (idato-posiciones_corr_drift[0])/(posiciones_corr_drift[-1]-posiciones_corr_drift[0])
            store[idato]    = factor_f
            recta_at[idato] = RMN_bajos.iloc[0] +  factor_f*(RMN_bajos.iloc[0]-RMN_bajos.iloc[-1]) 
            recta_bt[idato] = RMN_altos.iloc[0] -  factor_f*(RMN_altos.iloc[0]-RMN_altos.iloc[-1]) 
    
            val_combinado         = ((datos_brutos[variable_concentracion][idato]-recta_at[idato])/(recta_bt[idato]-recta_at[idato]))*(RMN_altos.iloc[0]-RMN_bajos.iloc[0]) + RMN_bajos.iloc[0]
    
            variable_drift[idato] = val_combinado*pte_RMN+t_indep_RMN
    
        variable_drift[variable_drift<0] = 0
        
        datos_corregidos[variables_run[ivariable]] = variable_drift
        
        return datos_corregidos
    
    
    



# VERSIONES ANTERIORES; 


# ###########################################################################
# ######## FUNCION PARA ENCONTRAR EL IDENTIFICADOR DE CADA REGISTRO  ########
# ###########################################################################

# def evalua_registros(datos,nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto):
    
#     # Recupera la tabla con los registros de los muestreos
#     con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
#     conn_psql        = create_engine(con_engine)
#     tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
#     tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
    
    
#     datos['id_muestreo_temp']  = numpy.zeros(datos.shape[0],dtype=int)
    
#     # si no hay ningun valor en la tabla de registro, meter directamente todos los datos registrados
#     if tabla_muestreos.shape[0] == 0:
    
#         # genera un dataframe con las variables que interesa introducir en la base de datos
#         exporta_registros                    = datos[['id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','configuracion_perfilador','configuracion_superficie']]
#         # añade el indice de cada registro
#         indices_registros                    = numpy.arange(1,(exporta_registros.shape[0]+1))    
#         exporta_registros['id_muestreo']     = indices_registros
#         # renombra la columna con información de la estación muestreada
#         exporta_registros                    = exporta_registros.rename(columns={"id_estacion_temp":"estacion",'id_salida':'salida_mar'})
#         # # añade el nombre del muestreo
#         exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
#         for idato in range(exporta_registros.shape[0]):    
#             nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'][idato]]['nombre_estacion'].iloc[0]
#             if datos['prof_referencia'][idato] is not None:
#                 str_profundidad = str(round(datos['prof_referencia'][idato]))
#             else:
#                 str_profundidad = str(round(datos['presion_ctd'][idato]))                
                
#             exporta_registros['nombre_muestreo'][idato]  = nombre_programa + '_' + datos['fecha_muestreo'][idato].strftime("%Y_%m_%d")  + '_E' + str(nombre_estacion) + '_' + str_profundidad
#             datos['id_muestreo_temp'] [idato]            = idato + 1
            
            
#         # Inserta en base de datos        
#         exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
#         exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append') 
    
#     # En caso contrario hay que ver registro a registro, si ya está incluido en la base de datos
#     else:
    
#         ultimo_registro_bd         = max(tabla_muestreos['id_muestreo'])
#         datos['io_nuevo_muestreo'] = numpy.ones(datos.shape[0],dtype=int)

#         for idato in range(datos.shape[0]):

#             for idato_existente in range(tabla_muestreos.shape[0]):
                
#                 # Registro ya incluido, recuperar el identificador
#                 if tabla_muestreos['estacion'][idato_existente] == datos['id_estacion_temp'][idato] and tabla_muestreos['fecha_muestreo'][idato_existente] == datos['fecha_muestreo'][idato] and  tabla_muestreos['hora_muestreo'][idato_existente] == datos['hora_muestreo'][idato] and  tabla_muestreos['presion_ctd'][idato_existente] == datos['presion_ctd'][idato] and  tabla_muestreos['configuracion_perfilador'][idato_existente] == datos['configuracion_perfilador'][idato] and  tabla_muestreos['configuracion_superficie'][idato_existente] == datos['configuracion_superficie'][idato]:
#                     datos['id_muestreo_temp'] [idato] =  tabla_muestreos['id_muestreo'][idato_existente]    
#                     datos['io_nuevo_muestreo'][idato] = 0
            
#             # Nuevo registro
#             if datos['io_nuevo_muestreo'][idato] == 1:
#                 # Asigna el identificador (siguiente al máximo disponible)
#                 ultimo_registro_bd                = ultimo_registro_bd + 1
#                 datos['id_muestreo_temp'][idato]  = ultimo_registro_bd  
               
        
#         if numpy.count_nonzero(datos['io_nuevo_muestreo']) > 0:
        
#             # Genera un dataframe sólo con los valores nuevos, a incluir (io_nuevo_muestreo = 1)
#             nuevos_muestreos  = datos[datos['io_nuevo_muestreo']==1]
#             # Mantén sólo las columnas que interesan
#             exporta_registros = nuevos_muestreos[['id_muestreo_temp','id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','configuracion_perfilador','configuracion_superficie']]
                        
#             # Cambia el nombre de la columna de estaciones
#             exporta_registros = exporta_registros.rename(columns={"id_estacion_temp":"estacion","id_muestreo_temp":"id_muestreo",'id_salida':'salida_mar'})
#             # Indice temporal
#             exporta_registros['indice_temporal'] = numpy.arange(0,exporta_registros.shape[0])
#             exporta_registros.set_index('indice_temporal',drop=True,append=False,inplace=True)
#             # Añade el nombre del muestreo
#             exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
#             for idato in range(exporta_registros.shape[0]):    
#                 nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'][idato]]['nombre_estacion'].iloc[0]
#                 if datos['prof_referencia'][idato] is not None:
#                     str_profundidad = str(round(datos['prof_referencia'][idato]))
#                 else:
#                     str_profundidad = str(round(datos['presion_ctd'][idato]))                
                

#             # # Inserta el dataframe resultante en la base de datos 
#             exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
#             exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append')    
    
#     conn_psql.dispose() # Cierra la conexión con la base de datos 
    
#     return datos



#######
 
    
#     import numpy
#     import pandas
#     import datetime
#     from pyproj import Proj
#     import math
#     import psycopg2
#     import pandas.io.sql as psql
#     from sqlalchemy import create_engine
#     import json
#     datos = datos_radiales_corregido
#     nombre_programa = programa_muestreo
    
#     # Recupera la tabla con los registros de los muestreos
#     con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
#     conn_psql        = create_engine(con_engine)
#     tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
#     conn_psql.dispose() # Cierra la conexión con la base de datos 
    
#     # Predimensiona vectores
#     datos['id_muestreo']       = numpy.zeros(datos.shape[0],dtype=int)

#     # Instrucción de inserción
#     instruccion_sql = '''INSERT INTO muestreos_discretos (nombre_muestreo,fecha_muestreo,hora_muestreo,salida_mar,estacion,num_cast,botella,prof_referencia,presion_ctd,configuracion_perfilador,configuracion_superficie)
#     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id_muestreo) DO UPDATE SET (nombre_muestreo,num_cast,botella,prof_referencia) = ROW(EXCLUDED.nombre_muestreo,EXCLUDED.num_cast,EXCLUDED.botella,EXCLUDED.prof_referencia);''' 

#     conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
#     cursor = conn.cursor()
    
#     for idato in range(datos.shape[0]):

#         # Determina el nombre de la estación muestreada
#         nombre_estacion = tabla_estaciones['nombre_estacion'][tabla_estaciones['id_estacion']==datos['id_estacion_temp'][idato]].loc[0]        

#         # Determina el nombre del muestreo        
#         if datos['prof_referencia'][idato] is not None:
#             str_profundidad = str(round(datos['prof_referencia'][idato]))
#         else:
#             str_profundidad = str(round(datos['presion_ctd'][idato]))                            
#         nombre_muestreo = nombre_programa + '_' + datos['fecha_muestreo'][idato].strftime("%Y_%m_%d")  + '_EST:' + str(nombre_estacion) + '_PROF:' + str_profundidad        

#         # Convierte las variables enteras a números enteros
#         if datos['num_cast'][idato] is None:
#             numero_cast = None
#         else:
#             numero_cast = int(datos['num_cast'][idato])
        
#         if datos['botella'][idato] is None:
#             id_botella = None
#         else:
#             id_botella = int(datos['botella'][idato])
            
#         datos_insercion = (nombre_muestreo,datos['fecha_muestreo'][idato],datos['hora_muestreo'][idato],int(datos['id_salida'][idato]),int(datos['id_estacion_temp'][idato]),numero_cast,id_botella,int(datos['prof_referencia'][idato]),datos['presion_ctd'][idato],int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato]))

#         # Inserta el registro en la base de datos
#         cursor.execute(instruccion_sql, datos_insercion)
#         conn.commit()
        
#         # Recupera el identificador del registro
#         instruccion_sql = "SELECT id_muestreo FROM muestreos_discretos WHERE estacion = %s AND fecha_muestreo = %s ;" 
#         cursor.execute(instruccion_sql,(int(datos['id_estacion_temp'][idato]),datos['fecha_muestreo'][idato]))
#         id_muestreo =cursor.fetchone()[0]
#         conn.commit()

# #estacion,fecha_muestreo,hora_muestreo,salida_mar,presion_ctd,configuracion_superficie,configuracion_perfilador

#         datos['id_muestreo'][idato] = id_muestreo
        
#     cursor.close()
#     conn.close()

  
 
 
    
    
