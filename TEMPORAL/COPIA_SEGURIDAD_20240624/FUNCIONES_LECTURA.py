# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""

import numpy
import pandas
import datetime
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine
import re
import math
import seawater





pandas.options.mode.chained_assignment = None


#############################################################################
######## FUNCION PARA LEER DATOS CON EL FORMATO DE CAMPAÑA RADIALES  ########
############################################################################# 

def lectura_datos_radiales(nombre_archivo,direccion_host,base_datos,usuario,contrasena,puerto):
   
    ## CARGA LA INFORMACION CONTENIDA EN EL EXCEL
    
    # Importa el .xlsx
    datos_radiales = pandas.read_excel(nombre_archivo, 'data',index_col=None)
    
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

    try:

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
    
    except:
        pass
    
    
    # Asigna el valor del cast. Si es un texto no asigna valor
    datos_radiales['num_cast'] = numpy.ones(datos_radiales.shape[0],dtype=int)
#    datos_radiales['num_cast'] = [None]*datos_radiales.shape[0]



    #listado_variables_elimina
    #datos_radiales = datos_radiales.drop(columns=['CTDOXY_CAL','CTDOXY_CAL_FLAG_W','CTDFLOUR_SCUFA', 'CTDFLUOR_AFL','CTDFLUOR_SP','CTDFLOUR_SCUFA_FLAG_W','CTDFLUOR_AFL_FLAG_W','CTDFLUOR_SP_FLAG_W','PHTS25P0_UNPUR','PHTS25P0_UNPUR_FLAG_W','PHTS25P0_PUR','PHTS25P0_PUR_FLAG_W'])
    #datos_radiales = datos_radiales.drop(columns=['configuracion_perfilador','configuracion_superficie'])

     
    # Renombra las columnas para mantener un mismo esquema de nombres   
    datos_radiales = datos_radiales.rename(columns={"DATE": "fecha_muestreo", "STNNBR": "estacion", 
                                                    "LATITUDE":"latitud","LONGITUDE":"longitud","BTLNBR":"botella","CTDPRS":"presion_ctd",
                                                    "CTDTMP":"temperatura_ctd","CTDSAL":"salinidad_ctd","CTDSAL_FLAG_W":"salinidad_ctd_qf",
                                                    "CTDOXY":"oxigeno_ctd","CTDOXY_FLAG_W":"oxigeno_ctd_qf","CTDPAR":"par_ctd","CTDPAR_FLAG_W":"par_ctd_qf",
                                                    "CTDTURB":"turbidez_ctd","CTDTURB_FLAG_W":"turbidez_ctd_qf","OXYGEN":"oxigeno_wk","OXYGEN_FLAG_W":"oxigeno_wk_qf",
                                                    "SILCAT":"silicato","SILCAT_FLAG_W":"silicato_qf","NITRAT":"nitrato","NITRAT_FLAG_W":"nitrato_qf","NITRIT":"nitrito","NITRIT_FLAG_W":"nitrito_qf",
                                                    "PHSPHT":"fosfato","PHSPHT_FLAG_W":"fosfato_qf","TCARBN":"carbono_inorganico_total","TCARBN_FLAG_W":"carbono_inorganico_total_qf","ALKALI":"alcalinidad","ALKALI_FLAG_W":"alcalinidad_qf",                                                   
                                                    "R_CLOR":"r_clor","R_CLOR_FLAG_W":"r_clor_qf","R_PER":"r_per","R_PER_FLAG_W":"r_per_qf","CO3_TMP":"co3_temp"
                                                    })    
    
    # Ojo con esto, cuando una de las medidas está mal, estropea
    datos_radiales['nitrogeno_inorganico_total']    = [None]*datos_radiales.shape[0]
    datos_radiales['nitrogeno_inorganico_total_qf'] = 9
    for idato in range(datos_radiales.shape[0]):
       if datos_radiales['nitrato_qf'][idato] == 2 and datos_radiales['nitrito_qf'][idato] == 2:
           datos_radiales['nitrogeno_inorganico_total'][idato]    = datos_radiales['nitrato'][idato] + datos_radiales['nitrito'][idato]
           datos_radiales['nitrogeno_inorganico_total_qf'][idato] = 2
    
    # Añade una columan con el QF de la temperatura, igual al de la salinidad
    datos_radiales['temperatura_ctd_qf'] = datos_radiales['salinidad_ctd_qf'] 
 
    # Añade una columan con la hora, aunque sea nula
    datos_radiales['hora_muestreo'] = None
    
  
    # # Modifica el numero de la estacion a un entero
    try:
        datos_radiales['estacion'] = datos_radiales['estacion'].astype(int)
    except:
        pass
  
    # Modifica el nombre de la estación para igualarlo al criterio utilizado en la base de datos
    for idato in range(datos_radiales.shape[0]):
        # Define las profundidades de referencia en cada estación
        if str(datos_radiales['estacion'].iloc[idato]) == '2': #Estación 2
            datos_radiales['estacion'].iloc[idato] = 'E2CO'
        if str(datos_radiales['estacion'][idato]) == '4': #Estación 2        
            datos_radiales['estacion'].iloc[idato] = 'E4CO' 
        if str(datos_radiales['estacion'][idato]).upper()  == '3C': #Estación 2        
            datos_radiales['estacion'].iloc[idato] = 'E3CCO'     
        if str(datos_radiales['estacion'][idato]).upper() == '3B': #Estación 2        
            datos_radiales['estacion'].iloc[idato] = 'E3BCO'  
        if str(datos_radiales['estacion'][idato]).upper() == '3A': #Estación 2        
            datos_radiales['estacion'].iloc[idato] = 'E3ACO' 
        if str(datos_radiales['estacion'][idato]) == '3': #Estación 2        
            datos_radiales['estacion'].iloc[idato] = 'E3CO' 
    
    # Añade una columna con la profundidad de referencia
    datos_radiales['prof_teorica'] = numpy.zeros(datos_radiales.shape[0],dtype=int)
    for idato in range(datos_radiales.shape[0]):
        # Define las profundidades de referencia en cada estación
        if str(datos_radiales['estacion'][idato]) == 'E2CO': #Estación 2
            profundidades_referencia = numpy.asarray([0,5,10,20,30,40,70])
        if str(datos_radiales['estacion'][idato]) == 'E4CO': #Estación 4        
            profundidades_referencia = numpy.asarray([0,4,8,12,18]) 
        if str(datos_radiales['estacion'][idato]) == 'E3CCO': #Estación 3c        
             profundidades_referencia = numpy.asarray([0,5,10,20,30,35,40]) 
             
        # Encuentra la profundidad de referencia más cercana a cada dato
        idx = (numpy.abs(profundidades_referencia - datos_radiales['presion_ctd'][idato])).argmin()
        datos_radiales['prof_teorica'][idato] =  profundidades_referencia[idx]

#     # Añade una columna con el nombre del muestreo
#     datos_radiales['prof_teorica'] = numpy.zeros(datos_radiales.shape[0],dtype=int)
#     for idato in range(datos_radiales.shape[0]):
#         # Define las profundidades de referencia en cada estación
#         if str(int(datos_radiales['estacion'][idato])) == '2': #Estación 2
             

# RADCOR_20210113_E2_C1_B7
    
    
    return datos_radiales
 

   

############################################################################
######## FUNCION PARA LEER DATOS HISTORICOS DE LA  CAMPAÑA RADIALES  #######
############################################################################ 
def lectura_radiales_historicos(nombre_archivo):
    
    # Importa el .xlsx
    datos_radiales = pandas.read_excel(nombre_archivo, 'datos',na_values='#N/A')
    
    # # Elimina la primera fila, con unidades de las distintas variables
    # datos_radiales = datos_radiales.iloc[1: , :]
    
    # Convierte las fechas de DATE a formato correcto
    datos_radiales['Fecha'] =  pandas.to_datetime(datos_radiales['Fecha'], format='%Y%m%d').dt.date
      
    # Define una columna índice
    indices_dataframe         = numpy.arange(0,datos_radiales.shape[0],1,dtype=int)
    datos_radiales['id_temp'] = indices_dataframe
    datos_radiales.set_index('id_temp',drop=True,append=False,inplace=True)
    
    # Cambia los nan por None
    datos_radiales = datos_radiales.replace(numpy.nan, None)
    
    # Corrige las concentraciones para pasarlas de mmol/m3 (o umol/l) a umol/kg
    
    # Calculo de densidades
    listado_variables_datos = datos_radiales.columns.values.tolist()
    if 'sigmat' in listado_variables_datos:
        datos_radiales['densidad'] = numpy.ones(datos_radiales.shape[0])
        for idato in range(datos_radiales.shape[0]):
            if datos_radiales['sigmat'].iloc[idato] is not None:
                datos_radiales['densidad'].iloc[idato]  =  1 + datos_radiales['sigmat'].iloc[idato]/1000
    else:
        datos_radiales['densidad'] = numpy.ones(datos_radiales.shape[0])        
        for idato in range(datos_radiales.shape[0]):
            if datos_radiales['S'].iloc[idato] is not None and datos_radiales['t'].iloc[idato] is not None:
                datos_radiales['densidad'].iloc[idato] = seawater.eos80.dens0(datos_radiales['S'].iloc[idato], datos_radiales['t'].iloc[idato])
        
    datos_radiales = datos_radiales.rename(columns={"Fecha": "fecha_muestreo", "Prof":"presion_ctd", "t":"temperatura_ctd","S":"salinidad_ctd","E":"par_ctd", 
                                                    "O2 umol/kg":"oxigeno_wk","Cla":"clorofila_a","ID_estacion":"estacion","Clb":"clorofila_b","Clc":"clorofila_c","PP":"prod_primaria","COP":"carbono_organico_particulado",'NOP':'nitrogeno_organico_particulado'})
    
    
    # correccion
    datos_radiales['nitrogeno_inorganico_total']      = [None]*datos_radiales.shape[0]
    datos_radiales['nitrato']  = [None]*datos_radiales.shape[0]
    datos_radiales['nitrito']  = [None]*datos_radiales.shape[0]
    datos_radiales['amonio']   = [None]*datos_radiales.shape[0]
    datos_radiales['fosfato']  = [None]*datos_radiales.shape[0]
    datos_radiales['silicato'] = [None]*datos_radiales.shape[0]
    
        
    datos_radiales['nitrogeno_inorganico_total_qf']      = int(2)
    datos_radiales['nitrato_qf']  = int(2)
    datos_radiales['nitrito_qf']  = int(2)
    datos_radiales['amonio_qf']   = int(2)
    datos_radiales['fosfato_qf']  = int(2)
    datos_radiales['silicato_qf'] = int(2)
    
    datos_radiales['botella']       = [None]*datos_radiales.shape[0]
    datos_radiales['hora_muestreo'] = [None]*datos_radiales.shape[0]
    datos_radiales['prof_teorica']  = [None]*datos_radiales.shape[0]
    datos_radiales['num_cast']      = [None]*datos_radiales.shape[0]
    
    datos_radiales['salinidad_ctd_qf']   = int(2)
    datos_radiales['temperatura_ctd_qf'] = int(2)
    datos_radiales['par_ctd_qf']         = int(2)
    
    datos_radiales['oxigeno_wk_qf']      = int(2)
    datos_radiales['clorofila_a_qf']     = int(2)
    datos_radiales['clorofila_b_qf']     = int(2)
    datos_radiales['clorofila_c_qf']     = int(2)
    
    for idato in range(datos_radiales.shape[0]):
        if datos_radiales['NO3'].iloc[idato] is not None:
            datos_radiales['nitrato'].iloc[idato]  = datos_radiales['NO3'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
        else:
            datos_radiales['nitrato_qf'].iloc[idato] = int(9)
              
        if datos_radiales['NO2'].iloc[idato] is not None:
            datos_radiales['nitrito'].iloc[idato]  = datos_radiales['NO2'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
        else:
            datos_radiales['nitrito_qf'].iloc[idato]  = int(9)       
        
        if datos_radiales['NH4'].iloc[idato] is not None:
            datos_radiales['amonio'].iloc[idato]  = datos_radiales['NH4'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
        else:
            datos_radiales['amonio_qf'].iloc[idato]  = int(9) 
        
        if datos_radiales['PO4'].iloc[idato] is not None:
            datos_radiales['fosfato'].iloc[idato]  = datos_radiales['PO4'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
        else:
            datos_radiales['fosfato_qf'].iloc[idato]  = int(9) 
            
        if datos_radiales['SiO2'].iloc[idato] is not None:
            datos_radiales['silicato'].iloc[idato]  = datos_radiales['SiO2'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
        else:
            datos_radiales['silicato_qf'].iloc[idato]  = int(9)   
            
            
    # QF
    if 'NO3flag' in listado_variables_datos:
        for idato in range(datos_radiales.shape[0]):
            if datos_radiales['NO3flag'].iloc[idato] is not None:
                datos_radiales['nitrato_qf'].iloc[idato]  = datos_radiales['NO3flag'].iloc[idato]  

    if 'NO2flag' in listado_variables_datos:
        for idato in range(datos_radiales.shape[0]):                
            if datos_radiales['NO2flag'].iloc[idato] is not None:
                datos_radiales['nitrito_qf'].iloc[idato]  = datos_radiales['NO2flag'].iloc[idato]  
                
    if 'NH4flag' in listado_variables_datos:   
        for idato in range(datos_radiales.shape[0]):              
            if datos_radiales['NH4flag'].iloc[idato] is not None:
                datos_radiales['amonio_qf'].iloc[idato]  = datos_radiales['NH4flag'].iloc[idato]
                
    if 'PO4flag' in listado_variables_datos: 
        for idato in range(datos_radiales.shape[0]):                 
            if datos_radiales['PO4flag'].iloc[idato] is not None:
                datos_radiales['fosfato_qf'].iloc[idato]  = datos_radiales['PO4flag'].iloc[idato]        
                
    if 'SiO2flag' in listado_variables_datos: 
        for idato in range(datos_radiales.shape[0]):               
            if datos_radiales['SiO2flag'].iloc[idato] is not None:
                datos_radiales['silicato_qf'].iloc[idato]  = datos_radiales['SiO2flag'].iloc[idato]           
    
        # calculo del TIN 
        if datos_radiales['nitrato'].iloc[idato] is not None and datos_radiales['nitrito'].iloc[idato] is not None:
            datos_radiales['nitrogeno_inorganico_total'].iloc[idato]  = datos_radiales['nitrato'].iloc[idato] + datos_radiales['nitrito'].iloc[idato]
            if datos_radiales['amonio'].iloc[idato] is not None:
                datos_radiales['nitrogeno_inorganico_total'].iloc[idato] = datos_radiales['nitrogeno_inorganico_total'].iloc[idato] + datos_radiales['amonio'].iloc[idato]
        else:
            datos_radiales['nitrogeno_inorganico_total_qf'].iloc[idato] = int(9) 
        
        # Reviso los QF
        if datos_radiales['salinidad_ctd'].iloc[idato] is None:
            datos_radiales['salinidad_ctd_qf'].iloc[idato] = int(9) 
        if datos_radiales['temperatura_ctd'].iloc[idato] is None:
            datos_radiales['temperatura_ctd_qf'].iloc[idato] = int(9) 
        if datos_radiales['par_ctd'].iloc[idato] is None:
            datos_radiales['par_ctd_qf'].iloc[idato] = int(9) 
        if datos_radiales['oxigeno_wk'].iloc[idato] is None:
            datos_radiales['oxigeno_wk_qf'].iloc[idato] = int(9) 
        if datos_radiales['clorofila_a'].iloc[idato] is None:
            datos_radiales['clorofila_a_qf'].iloc[idato] = int(9) 
        if datos_radiales['clorofila_b'].iloc[idato] is None:
            datos_radiales['clorofila_b_qf'].iloc[idato] = int(9) 
        if datos_radiales['clorofila_c'].iloc[idato] is None:
            datos_radiales['clorofila_c_qf'].iloc[idato] = int(9)          

    # Asigna numero de botella
    datos_radiales['botella'] = None
    df_datos  = pandas.DataFrame()
    listado_salidas = datos_radiales['fecha_muestreo'].unique()   
    for isalida in range(len(listado_salidas)):
        
        subset_salida = datos_radiales[datos_radiales['fecha_muestreo'] == listado_salidas[isalida]]
        
        listado_estaciones = subset_salida['estacion'].unique()
        
        for iestacion in range(len(listado_estaciones)):
        
            datos_estacion = subset_salida[subset_salida['estacion']==listado_estaciones[iestacion]]
            datos_estacion = datos_estacion.sort_values(by=['presion_ctd'])
            
            for idato in range(datos_estacion.shape[0]):
                
                datos_estacion['botella'].iloc[idato] = int(idato + 1)
                
            df_datos = pandas.concat([df_datos, datos_estacion])                 

    datos_radiales =  df_datos      
    
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
             
    datos_pelacus['nitrogeno_inorganico_total_qf']    = 9
    datos_pelacus['cast_temp'] = [None]*datos_pelacus.shape[0]
    for idato in range(datos_pelacus.shape[0]):
        if isinstance(datos_pelacus['cast'].iloc[idato], str) and datos_pelacus['cast'].iloc[idato][0:5] == 'PEL03':
            listado_palabras = datos_pelacus['cast'].iloc[idato].split('_')
            texto_busqueda   = listado_palabras[1]
        else:
            texto_busqueda = datos_pelacus['cast'].iloc[idato]
        try:
            datos_pelacus['cast_temp'].iloc[idato] = int("".join(filter(str.isdigit, texto_busqueda)))
        except:
            datos_pelacus['cast_temp'].iloc[idato] = datos_pelacus['cast'].iloc[idato] 
    
        if math.isnan(datos_pelacus['cast_temp'].iloc[idato]): 
            datos_pelacus['cast_temp'].iloc[idato] = 1
            
        if math.isnan(datos_pelacus['NO3T_flag'].iloc[idato]):
            if not math.isnan(datos_pelacus['NO3'].iloc[idato]):
                datos_pelacus['NO3T_flag'].iloc[idato] = int(1)
        if math.isnan(datos_pelacus['NO3'].iloc[idato]):
            datos_pelacus['NO3T_flag'].iloc[idato] = int(9)
                
        if math.isnan(datos_pelacus['NO2_flag'].iloc[idato]):
            if not math.isnan(datos_pelacus['NO2'].iloc[idato]):
                datos_pelacus['NO2_flag'].iloc[idato] = int(1)
        if math.isnan(datos_pelacus['NO2'].iloc[idato]):
            datos_pelacus['NO2_flag'].iloc[idato] = int(9)
    
        if math.isnan(datos_pelacus['NH4_flag'].iloc[idato]):
            if not math.isnan(datos_pelacus['NH4'].iloc[idato]):
                datos_pelacus['NH4_flag'].iloc[idato] = int(1)
        if math.isnan(datos_pelacus['NH4'].iloc[idato]):
            datos_pelacus['NH4_flag'].iloc[idato] = int(9)
    
        if math.isnan(datos_pelacus['PO4_flag'].iloc[idato]):
            if not math.isnan(datos_pelacus['PO4'].iloc[idato]):
                datos_pelacus['PO4_flag'].iloc[idato] = int(1)
        if math.isnan(datos_pelacus['PO4'].iloc[idato]):
            datos_pelacus['PO4_flag'].iloc[idato] = int(9)
    
        if math.isnan(datos_pelacus['SiO2_flag'].iloc[idato]):
            if not math.isnan(datos_pelacus['SiO2'].iloc[idato]):
                datos_pelacus['SiO2_flag'].iloc[idato] = int(1)
        if math.isnan(datos_pelacus['SiO2'].iloc[idato]):
            datos_pelacus['SiO2_flag'].iloc[idato] = int(9)
            
        if not isinstance(datos_pelacus['cast'].iloc[idato], str) and math.isnan(datos_pelacus['cast'].iloc[idato]) is True:    
            datos_pelacus['cast'].iloc[idato] = 1

    datos_pelacus['presion_ctd'] = numpy.zeros(datos_pelacus.shape[0])
    datos_pelacus['Prof_real']   = datos_pelacus['Prof_real'].replace({numpy.nan:None})
    datos_pelacus['Prof_teor.']  = datos_pelacus['Prof_teor.'].replace({numpy.nan:None})
    for idato in range(datos_pelacus.shape[0]):
        if datos_pelacus['Prof_real'][idato] is not None:
            datos_pelacus['presion_ctd'][idato] = datos_pelacus['Prof_real'][idato]
        if datos_pelacus['Prof_real'][idato] is None and datos_pelacus['Prof_teor.'][idato] is not None:
            datos_pelacus['presion_ctd'][idato] = datos_pelacus['Prof_teor.'][idato]        
        if datos_pelacus['Prof_real'][idato] is None and datos_pelacus['Prof_teor.'][idato] is None:
            datos_pelacus['presion_ctd'][idato] = -999   
            
    # Asigna la profundidad teórica como la de referencia
    datos_pelacus['prof_teorica'] = datos_pelacus['Prof_teor.']
  
    datos_pelacus = datos_pelacus.drop(columns=['Prof_est','Prof_real', 'Prof_teor.'])  
  
    # Asigna el valor del cast. Si es un texto no asigna valor
    
    # datos_pelacus['num_cast'] = numpy.ones(datos_pelacus.shape[0],dtype=int)
    # for idato in range(datos_pelacus.shape[0]):
    #     try: # Si es entero
    #         texto       = datos_pelacus['cast'][idato]      
    #         datos_pelacus['num_cast'][idato] = int(re.findall(r'\d+',texto)[0])            
    #     except:
    #         datos_pelacus['num_cast'][idato] = None

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
    datos_pelacus = datos_pelacus.drop(columns=['cast',"campaña"])           
    datos_pelacus = datos_pelacus.rename(columns={"fecha":"fecha_muestreo","hora":"hora_muestreo","estación":"estacion",
                                                  "Latitud":"latitud","Longitud":"longitud","t_CTD":"temperatura_ctd","Sal_CTD":"salinidad_ctd","SiO2":"silicato","SiO2_flag":"silicato_qf",
                                                  "NO3":"nitrato","NO3T_flag":"nitrato_qf","NO2":"nitrito","NO2_flag":"nitrito_qf","NH4":"amonio","NH4_flag":"amonio_qf","PO4":"fosfato","PO4_flag":"fosfato_qf","Cla":"clorofila_a",
                                                  "cast_temp":"num_cast"})

    datos_pelacus['botella'] = None
    
    #datos_pelacus['estacion'] = datos_pelacus['estacion'].astype(int)
    

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
        datos_radprof['Sta'][idato]             = str(datos_radprof['Sta'][idato])
        
    # Calcula la columna de NO3 a partir de la suma NO3+NO2
    datos_radprof['nitrato']    =  datos_radprof['NO3+NO2 umol/Kg'] - datos_radprof['NO2 umol/kg']
    datos_radprof['nitrato_qf'] =  1 #datos_radprof['Flag_TON'] 
       

    # Renombra las columnas para mantener una denominación homogénea
    datos_radprof = datos_radprof.rename(columns={"Sta":"estacion","Niskin":"botella","Cast":'num_cast',
                                                  "Lat":"latitud","Lon":"longitud","CTDPRS":"presion_ctd","CTDtemp":"temperatura_ctd","SALCTD":"salinidad_ctd",
                                                  "SiO2 umol/Kg":"silicato","Flag_SiO2":"silicato_qf",
                                                  "NO2 umol/kg":"nitrito","Flag_NO2":"nitrito_qf","PO4 umol/Kg":"fosfato","Flag_PO4":"fosfato_qf","ID":"id_externo"
                                                  })

    # Mantén solo las columnas que interesan
    datos_radprof_recorte = datos_radprof[['estacion','botella','fecha_muestreo','hora_muestreo','latitud','longitud','presion_ctd','num_cast','temperatura_ctd','salinidad_ctd',
                                            'nitrato','nitrato_qf','nitrito','nitrito_qf','silicato','silicato_qf','fosfato','fosfato_qf','id_externo']]
    

    # Añade qf a los datos del CTD
    listado_variables = datos_radprof_recorte.columns.tolist()
    if 'temperatura_ctd' in listado_variables and 'temperatura_ctd_qf' not in listado_variables:
        datos_radprof_recorte['temperatura_ctd_qf'] = 2
        
    if 'salinidad_ctd' in listado_variables and 'salinidad_ctd_qf' not in listado_variables:
        datos_radprof_recorte['salinidad_ctd_qf'] = 2        

    for idato in range(datos_radprof_recorte.shape[0]):
        if math.isnan(datos_radprof_recorte['temperatura_ctd'].iloc[idato]):
            datos_radprof_recorte['temperatura_ctd_qf'].iloc[idato] = 9
        if math.isnan(datos_radprof_recorte['salinidad_ctd'].iloc[idato]):
            datos_radprof_recorte['salinidad_ctd_qf'].iloc[idato] = 9            
            
    # Añade una columna con lat/lon muestreo
    datos_radprof_recorte['latitud_muestreo']=datos_radprof_recorte['latitud']
    datos_radprof_recorte['longitud_muestreo']=datos_radprof_recorte['longitud']   


    del(datos_radprof)

    return datos_radprof_recorte



############################################################################
######## FUNCION PARA LEER DATOS CON EL FORMATO DE CAMPAÑA RADCAN  ########
############################################################################ 
    
def lectura_datos_radcan(nombre_archivo):
  
    # Importa el .xlsx
   
    datos_radcan = pandas.read_excel(nombre_archivo, 'datos',dtype={'hora_muestreo': datetime.time})
    for idato in range(datos_radcan.shape[0]):
        datos_radcan['fecha_muestreo'][idato] = (datos_radcan['fecha_muestreo'][idato]).date()
    
    datos_radcan = datos_radcan.replace(numpy.nan, None)
    # datos_radcan['hora_muestreo'] = datos_radcan['hora_muestreo'].replace(numpy.nan, None)
    # datos_radcan['num_cast']      = datos_radcan['num_cast'].replace(numpy.nan, None)
    # datos_radcan['botella']       = datos_radcan['botella'].replace(numpy.nan, None)
    
    # Corrige valores de nutrientes <0 
    datos_radcan['nitrogeno_inorganico_total'][datos_radcan['nitrogeno_inorganico_total']<0]=0
    datos_radcan['nitrito'][datos_radcan['nitrito']<0]=0
    datos_radcan['fosfato'][datos_radcan['fosfato']<0]=0
    datos_radcan['silicato'][datos_radcan['silicato']<0]=0
    
    # Cambia el nombre de las estaciones
    for idato in range(datos_radcan.shape[0]):
        if datos_radcan['estacion'].iloc[idato].upper() == 'C1':
            datos_radcan['estacion'].iloc[idato]  = 'E1CU'
        if datos_radcan['estacion'].iloc[idato].upper() == 'C2':
            datos_radcan['estacion'].iloc[idato]  = 'E2CU'        
        if datos_radcan['estacion'].iloc[idato].upper() == 'C3':
            datos_radcan['estacion'].iloc[idato]  = 'E3CU'    
            
        if datos_radcan['estacion'].iloc[idato].upper() == 'G1':
            datos_radcan['estacion'].iloc[idato]  = 'E1GI'
        if datos_radcan['estacion'].iloc[idato].upper() == 'G2':
            datos_radcan['estacion'].iloc[idato]  = 'E2GI'        
        if datos_radcan['estacion'].iloc[idato].upper() == 'G3':
            datos_radcan['estacion'].iloc[idato]  = 'E3GI'  
        if datos_radcan['estacion'].iloc[idato].upper() == 'G4':
            datos_radcan['estacion'].iloc[idato]  = 'E4GI'   

        if datos_radcan['estacion'].iloc[idato].upper() == 'S1':
            datos_radcan['estacion'].iloc[idato]  = 'E1SA'
        if datos_radcan['estacion'].iloc[idato].upper() == 'S2':
            datos_radcan['estacion'].iloc[idato]  = 'E2SA'        
        if datos_radcan['estacion'].iloc[idato].upper() == 'S3':
            datos_radcan['estacion'].iloc[idato]  = 'E3SA'  
        if datos_radcan['estacion'].iloc[idato].upper() == 'S4':
            datos_radcan['estacion'].iloc[idato]  = 'E4SA'
        if datos_radcan['estacion'].iloc[idato].upper() == 'S5':
            datos_radcan['estacion'].iloc[idato]  = 'E5SA'
        if datos_radcan['estacion'].iloc[idato].upper() == 'S6':
            datos_radcan['estacion'].iloc[idato]  = 'E6SA'        
        if datos_radcan['estacion'].iloc[idato].upper() == 'S7':
            datos_radcan['estacion'].iloc[idato]  = 'E7SA'  
        if datos_radcan['estacion'].iloc[idato].upper() == 'S8':
            datos_radcan['estacion'].iloc[idato]  = 'E8SA'            
        if datos_radcan['estacion'].iloc[idato].upper() == 'S9':
            datos_radcan['estacion'].iloc[idato]  = 'E9SA'  
    return datos_radcan




######################################################
######## FUNCION PARA LEER DATOS DE PERFILES  ########
###################################################### 
    
def lectura_archivo_perfiles(datos_archivo):

    # Predimensionamientos
    listado_variables  = []
    datos_perfil       = []
    
    # Lee el archivo .cnv
    cast_muestreo      = 1 # Asinga este valor por si no se introdujo ningún dato en el muestreo
    fecha_muestreo     = None
    hora_muestreo      = None
    datetime_muestreo  = None
    datetime_sistema   = None
    lat_muestreo       = None
    lon_muestreo       = None
    
    for ilinea in range(len(datos_archivo)):
        texto_linea = datos_archivo[ilinea]
        
        if texto_linea[0:1] == '#' or texto_linea[0:1] == '*':            
 
            if texto_linea[0:8] == '** Cast:': # Línea con el número de cast
                listado_textos   = texto_linea.split(': ') 
                try:
                    cast_muestreo = int(listado_textos[-1])
                except:
                    pass
                
            if texto_linea[0:12] == '** Latitude:': # Línea con latitud del muestreo
                #lat_muestreo  = float(texto_linea[12:19])
                texto   = texto_linea.split(':') 
                if len(texto[-1]) > 1:
                    lat_muestreo  = float(texto[-1][0:6])
                    if texto_linea[-2] == 'S':
                        lat_muestreo = lat_muestreo*-1

            if texto_linea[0:13] == '** Longitude:': # Línea con latitud del muestreo
                #lon_muestreo  = float(texto_linea[13:20])
                texto   = texto_linea.split(':') 
                if len(texto[-1]) > 1:
                    lon_muestreo  = float(texto[-1][0:6])
                    if texto_linea[-2] == 'W':
                        lon_muestreo = lon_muestreo*-1   
                    
                    
            
            if texto_linea[0:22] == '* System UpLoad Time =': # Línea con hora del cast
                listado_textos   = texto_linea.split('= ') 
                try:
                    datetime_sistema = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S ')
                except:
                    datetime_sistema = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S')
                                        
                fecha_muestreo = datetime_sistema.date()
                hora_muestreo  = datetime_sistema.time()
                
            
            if texto_linea[0:14] == '* System UTC =': # Línea con hora del cast
                listado_textos    = texto_linea.split('= ')  
                try:
                    datetime_muestreo = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S ')
                except:
                    datetime_muestreo = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S')
                fecha_muestreo = datetime_muestreo.date()
                hora_muestreo  = datetime_muestreo.time()            
            
            
            
            
            if texto_linea[0:8] == '** Date:': # Línea con fecha del cast
                listado_textos   = texto_linea.split(':') 
                try:
                    fecha_muestreo = (datetime.datetime.strptime(listado_textos[-1],'%d/%m/%y ')).date()
                except:
                    fecha_muestreo = datetime.datetime.strptime(listado_textos[-1],'%d/%m/%y').date()


            if texto_linea[0:8] == '** Time:': # Línea con hora del cast
                listado_textos   = texto_linea.split('Time:') 
                try:
                    hora_muestreo = (datetime.datetime.strptime(listado_textos[-1],'%H:%M ')).time()
                except:
                    hora_muestreo = datetime.datetime.strptime(listado_textos[-1],'%H:%M').time()

                    
                            
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
                if nombre_variable == 'sigma-é00': 
                    nombre_variable = 'sigmat'   
                
                listado_variables  = listado_variables + [nombre_variable]
                            
        else:
            
            
            datos_linea = texto_linea.split() 
            listado_datos = [float(x) for x in datos_linea] 
            datos_perfil.append(listado_datos) 
            
            
    # Pasa los datos a un dataframe
    datos_perfil = pandas.DataFrame(datos_perfil, columns = listado_variables)
        
    # Genera un segundo dataframe con los datos ya en json
    df_perfiles        = pandas.DataFrame()
    
    df_temp            = datos_perfil[['presion_ctd','temperatura_ctd']]
    df_temp['qf_temp'] = 2
    json_temperatura   = df_temp.to_json()
    
    df_sal            = datos_perfil[['presion_ctd','salinidad_ctd']]
    df_sal['qf_sal']  = 2
    json_salinidad    = df_sal.to_json()     
    
    df_perfiles = pandas.DataFrame([[json_temperatura,json_salinidad]], columns=['temperatura_ctd','salinidad_ctd'])
    
    try:
        df_par                 = datos_perfil[['presion_ctd','par_ctd']]
        df_par['qf_par']       = 2
        json_par               = df_par.to_json()   
        
        df_perfiles['par_ctd'] = json_par
        
    except:
        pass

    try:
        df_fluor                         = datos_perfil[['presion_ctd','fluorescencia_ctd']]
        df_fluor['fluorescencia_ctd_qf'] = 2
        json_fluor                       = df_fluor.to_json()   
        
        df_perfiles['fluorescencia_ctd'] = json_fluor
        
    except:
        pass
    
    
    try:
        df_oxi                           = datos_perfil[['presion_ctd','oxigeno_ctd']]
        df_oxi['oxigeno_ctd_qf']         = 2
        json_oxi                         = df_oxi.to_json()   
        
        df_perfiles['oxigeno_ctd'] = json_oxi
        
    except:
        pass

    try:
        df_sigmat                           = datos_perfil[['presion_ctd','sigmat_ctd']]
        json_sigmat                         = df_sigmat.to_json()   
        
        df_perfiles['sigmat'] = json_sigmat
        
    except:
        pass    
    

    # Compón un dataframe con los metadatos del muestreo
    metadatos_muestreo    = [[fecha_muestreo,hora_muestreo,cast_muestreo,lat_muestreo,lon_muestreo]]
    datos_muestreo_perfil = pandas.DataFrame(metadatos_muestreo, columns=['fecha_muestreo', 'hora_muestreo','cast_muestreo','lat_muestreo','lon_muestreo'])
    
    return datos_perfil,df_perfiles,datos_muestreo_perfil    




    






######################################################################
######## FUNCION PARA LEER DATOS DE BOTELLAs (ARCHIVOS .BTL)  ########
######################################################################
def lectura_btl(nombre_archivo,datos_archivo):
 
    
    

    # Identifica la fecha del muestreo
    try: 
        fecha_salida_texto = nombre_archivo[0:8]
        fecha_salida       = datetime.datetime.strptime(fecha_salida_texto, '%Y%m%d').date()
    except:
        fecha_salida       = None

    
    
    # Genera las listas en las que se guardarán los datos si éstos existen
    datos_botella     = []
    datos_salinidad   = []
    datos_temperatura = []
    datos_presion     = []
    datos_PAR         = []
    datos_fluor       = []
    datos_O2          = []
    datos_tiempos     = []
    datos_sigmat      = []
    datos_turb        = []

    io_time_nmea = 0

    # Lee el archivo .btl y escribe la información de las botellas en un archivo temporal
    cast_muestreo          = 1 # Asinga este valor por si no se introdujo ningún dato en el muestreo
    fecha_muestreo_archivo = None
    for ilinea in range(len(datos_archivo)):
        texto_linea = datos_archivo[ilinea]
        if texto_linea[0:1] == '#' or texto_linea[0:1] == '*':
            

            if texto_linea[0:12] == '** Latitude:': # Línea con latitud del muestreo
                try:
                    lat_muestreo  = float(texto_linea[12:19])
                    if texto_linea[-1] == 'S':
                        lat_muestreo = lat_muestreo*-1
                except:
                    lat_muestreo = None
                    
            if texto_linea[0:17] == '* NMEA Latitude =': # Línea con latitud del muestreo (NMEA)
                listado_textos    = texto_linea.split('= ')     
                try:
                    texto_coordenadas   = listado_textos[1]
                    listado_coordenadas = texto_coordenadas.split(' ')
                    deg     = listado_coordenadas[0]
                    minutes = listado_coordenadas[1].split('.')[0]
                    seconds = listado_coordenadas[1].split('.')[1]
                    lat_muestreo = float(deg) + float(minutes)/60 + float(seconds)/(60*60)
                    if listado_coordenadas[2] == 'S':
                        lat_muestreo = lat_muestreo*-1
                except:
                    lat_muestreo = None


            if texto_linea[0:13] == '** Longitude:': # Línea con latitud del muestreo
                try:
                    lon_muestreo  = float(texto_linea[13:20])
                    if texto_linea[-1] == 'W':
                        lon_muestreo = lon_muestreo*-1
                except:
                    lon_muestreo = None
                    
            if texto_linea[0:18] == '* NMEA Longitude =': # Línea con latitud del muestreo (NMEA)
                listado_textos    = texto_linea.split('= ')     
                try:
                    texto_coordenadas   = listado_textos[1]
                    listado_coordenadas = texto_coordenadas.split(' ')
                    deg     = listado_coordenadas[0]
                    minutes = listado_coordenadas[1].split('.')[0]
                    seconds = listado_coordenadas[1].split('.')[1]
                    lon_muestreo = float(deg) + float(minutes)/60 + float(seconds)/(60*60)
                    if listado_coordenadas[2] == 'W':
                        lon_muestreo = lon_muestreo*-1
                except:
                    lon_muestreo = None
                    
                    
                    
            
            if texto_linea[0:19] == '* NMEA UTC (Time) =': # Línea con hora del cast 
                listado_textos   = texto_linea.split('= ') 
                try:
                    datetime_muestreo = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S ')
                except:
                    datetime_muestreo = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S')

                fecha_muestreo_archivo = datetime_muestreo.date()
                io_time_nmea  = 1
                offset_tiempo = 0
                   
                    
            if texto_linea[0:22] == '* System UpLoad Time =' and io_time_nmea == 0: # Línea con hora del cast
                listado_textos   = texto_linea.split('= ') 
                try:
                    datetime_sistema = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S ')
                except:
                    datetime_sistema = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S')
                    
            if texto_linea[0:14] == '* System UTC =' and io_time_nmea == 0: # Línea con hora del cast
                listado_textos    = texto_linea.split('= ')  
                try:
                    datetime_muestreo = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S ')
                except:
                    datetime_muestreo = datetime.datetime.strptime(listado_textos[-1],'%b %d %Y %H:%M:%S')
                offset_tiempo     = (datetime_sistema - datetime_muestreo).seconds
                fecha_muestreo_archivo = datetime_muestreo.date()
                
                
                
                
                

            if texto_linea[0:8] == '** Cast:': # Línea con el número de cast
#                cast_muestreo = int(texto_linea[8:len(texto_linea)])
                cast_muestreo = int(texto_linea[8:12])
            
        else:
    
            # Separa las cabeceras de las medidas de oxigeno si existen y están juntas 
            if 'Sbeox0VSbeox0Mm/Kg' in texto_linea: 
                texto_linea = texto_linea.replace('Sbeox0VSbeox0Mm/Kg', 'Sbeox0V Sbeox0Mm/Kg')

            if 'FlECO-AFLTurbWETntu0' in texto_linea: 
                texto_linea = texto_linea.replace('FlECO-AFLTurbWETntu0', 'FlECO-AFLTurbWET ntu0')                

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
                    try:
                        indice_presion   = datos_linea.index("PrdM")
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
                    try:
                        indice_fluor    = datos_linea.index("FlECO-AFLTurbWET")  
                        io_fluor        = 1                        
                    except:
                        indice_fluor    =  None 
                        io_fluor        = 0                    
                
                try:
                    indice_O2       = datos_linea.index("Sbox0Mm/Kg")#datos_linea.index("Sbeox0Mm/Kg")
                    io_O2           = 1                   
                except:
                    indice_O2       =  None  
                    io_O2           = 0  

                try:
                    indice_sigmat   = datos_linea.index("Sigma-t00")
                    io_sigmat       = 1                   
                except:
                    indice_sigmat   =  None  
                    io_sigmat       = 0    
                    
                try:
                    indice_turb   = datos_linea.index("ntu0")
                    io_turb       = 1                   
                except:
                    indice_turb   =  None  
                    io_turb       = 0   
                    
                    
    
    
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
                    if io_sigmat == 1:
                        datos_sigmat.append(float(datos_linea[indice_sigmat + 2])) 
                    if io_turb == 1:
                        datos_turb.append(float(datos_linea[indice_turb + 2])) 
                                    
                else: # Linea con los tiempos de cierre
                
                    hora_ciere                = datetime.datetime.strptime(datos_linea[0],'%H:%M:%S').time() 
                    datetime_cierre           = datetime.datetime.combine(datetime_muestreo.date(),hora_ciere)
                    datetime_cierre_corregido = datetime_cierre - datetime.timedelta(seconds=offset_tiempo)    
                    hora_ciere_corregida      = datetime_cierre_corregido.time()
                    datos_tiempos.append(hora_ciere_corregida)
    
    
    # Comprueba que la fecha contenida en el archivo y la del nombre del archivo son la misma
    if fecha_muestreo_archivo is not None and fecha_salida is not None and fecha_muestreo_archivo != fecha_salida:
    
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
        datos_botellas['temperatura_ctd_qf'] = int(1)
        datos_botellas['salinidad_ctd_qf']   = int(1)
        
        if io_par == 1:
            datos_botellas['par_ctd']              =  datos_PAR
            datos_botellas['par_ctd_qf']           = int(1)
        if io_fluor == 1:
            datos_botellas['fluorescencia_ctd']    =  datos_fluor
            datos_botellas['fluorescencia_ctd_qf'] = int(1)
        if io_O2 == 1:
            datos_botellas['oxigeno_ctd']          =  datos_O2
            datos_botellas['oxigeno_ctd_qf']       = int(1)
        if io_sigmat == 1:
            datos_botellas['sigmat']               =  datos_sigmat            
        if io_turb == 1:
            datos_botellas['turbidez_ctd']         =  datos_turb
            datos_botellas['turbidez_ctd_qf']      = int(1)
                     
        # Añade informacion de lat/lon y fecha para que no elimine el registro durante el control de calidad
        datos_botellas['latitud_muestreo']         = lat_muestreo  
        datos_botellas['longitud_muestreo']        = lon_muestreo 
        datos_botellas['fecha_muestreo']           = fecha_muestreo_archivo
        datos_botellas['num_cast']                 = cast_muestreo
        
                
        mensaje_error = []
        
    return mensaje_error,datos_botellas,io_par,io_fluor,io_O2




# ################################################################
# ########### FUNCION PARA PROCESAR ARCHIVOS DEL TOC/TN ##########
# ################################################################    

def lectura_toc(archivo_toc):

    # Lectura del archivo con los resultados del TOC
    datos_archivo              = pandas.read_excel(archivo_toc,skiprows=25)            
    
    # Mantén un recorte con las variables que interesan
    datos_toc = datos_archivo[['estacion','botella','muestra','conc C','conc N']]
    
    # Subset con los datos correspondientes a muestras
    datos_muestras = datos_toc[datos_toc[['estacion','botella']].notna().all(axis=1)]
    
    datos_muestras         = datos_muestras.rename(columns={"conc C":'carbono_organico_total','conc N':"nitrogeno_total"})
       
    # Metadatos
    datos_archivo_completo = pandas.read_excel(archivo_toc) 

    lcw_c = None
    lcw_n = None
    dsr_c = None
    dsr_n = None
    for idato in range(datos_archivo.shape[0]):
        if 'lcw' in str(datos_archivo['muestra'].iloc[idato]).lower():
            lcw_c = datos_archivo['conc C'].iloc[idato]
            lcw_n = datos_archivo['conc N'].iloc[idato] 
        
        if 'dsr' in str(datos_archivo['muestra'].iloc[idato]).lower():
            dsr_c = datos_archivo['conc C'].iloc[idato]
            dsr_n = datos_archivo['conc N'].iloc[idato]   

    data = {
    'pte_carbono': [datos_archivo_completo.iloc[5].iloc[19]],
    'r2_carbono':  [datos_archivo_completo.iloc[7].iloc[19]],
    'area_blanco_carbono': [datos_archivo_completo.iloc[3].iloc[20]],
    'conc_blanco_carbono': [datos_archivo_completo.iloc[3].iloc[21]],
    'pte_nitrogeno': [datos_archivo_completo.iloc[14].iloc[19]],
    'r2_nitrogeno':  [datos_archivo_completo.iloc[16].iloc[19]],
    'area_blanco_nitrogeno': [datos_archivo_completo.iloc[12].iloc[20]],
    'conc_blanco_nitrogeno': [datos_archivo_completo.iloc[12].iloc[21]],
    'lcw_c': [lcw_c],
    'lcw_n': [lcw_n],
    'dsr_c': [dsr_c],
    'dsr_n': [dsr_n],    
    'fecha_analisis': [datos_archivo_completo.iloc[4].iloc[6].date()]}
    

    
    # for idato in range(datos_archivo.shape[0]):
    #     if 'lcw' in str(datos_archivo['muestra'].iloc[idato]).lower():
    #         data['lcw_c'].iloc[0] = datos_archivo['conc C'].iloc[idato]
    #         data['lcw_n'].iloc[0] = datos_archivo['conc N'].iloc[idato] 
        
    #     if 'dsr' in str(datos_archivo['muestra'].iloc[idato]).lower():
    #         data['dsr_c'].iloc[0] = datos_archivo['conc C'].iloc[idato]
    #         data['dsr_n'].iloc[0] = datos_archivo['conc N'].iloc[idato]              


    datos_analisis = pandas.DataFrame(data)

    return  datos_muestras,datos_analisis   


  
 
 
    
    
