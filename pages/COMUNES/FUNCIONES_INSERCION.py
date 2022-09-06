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
    
    datos_radiales = datos_radiales.drop(columns=['CTDOXY_CAL','CTDOXY_CAL_FLAG_W','EXPOCODE','CTDFLOUR_SCUFA', 'CTDFLUOR_AFL','CTDFLUOR_SP','CTDFLOUR_SCUFA_FLAG_W','CTDFLUOR_AFL_FLAG_W','CTDFLUOR_SP_FLAG_W'])
     
    # Renombra las columnas para mantener un mismo esquema de nombres   
    datos_radiales = datos_radiales.rename(columns={"DATE": "fecha_muestreo", "STNNBR": "estacion",
                                                    "LATITUDE":"latitud","LONGITUDE":"longitud","BTLNBR":"botella","CTDPRS":"profundidad",
                                                    "CTDTMP":"temperatura_ctd","CTDSAL":"salinidad_ctd","CTDSAL_FLAG_W":"salinidad_ctd_qf",
                                                    "CTDOXY":"oxigeno_ctd","CTDOXY_FLAG_W":"oxigeno_ctd_qf","CTDPAR":"par_ctd","CTDPAR_FLAG_W":"par_ctd_qf",
                                                    "CTDTURB":"turbidez_ctd","CTDTURB_FLAG_W":"turbidez_ctd_qf","OXYGEN":"oxigeno_wk","OXYGEN_FLAG_W":"oxigeno_wk_qf",
                                                    "SILCAT":"sio4","SILCAT_FLAG_W":"sio4_qf","NITRAT":"no3","NITRAT_FLAG_W":"no3_qf","NITRIT":"no2","NITRIT_FLAG_W":"no2_qf",
                                                    "PHSPHT":"po4","PHSPHT_FLAG_W":"po4_qf","TCARBN":"tcarbn","TCARBN_FLAG_W":"tcarbn_qf","ALKALI":"alkali","ALKALI_FLAG_W":"alkali_qf",
                                                    "PHTS25P0_UNPUR":"phts25P0_unpur","PHTS25P0_UNPUR_FLAG_W":"phts25P0_unpur_qf","PHTS25P0_PUR":"phts25P0_pur","PHTS25P0_PUR_FLAG_W":"phts25P0_pur_qf",
                                                    "R_CLOR":"r_clor","R_CLOR_FLAG_W":"r_clor_qf","R_PER":"r_per","R_PER_FLAG_W":"r_per_qf","CO3_TMP":"co3_temp"
                                                    })    
    
        
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
    datos_pelacus['profundidad'] = numpy.zeros(datos_pelacus.shape[0])
    for idato in range(datos_pelacus.shape[0]):
        if datos_pelacus['Prof_real'][idato] is not None:
            datos_pelacus['profundidad'][idato] = datos_pelacus['Prof_real'][idato]
        if datos_pelacus['Prof_real'][idato] is None and datos_pelacus['Prof_teor.'][idato] is not None:
            datos_pelacus['profundidad'][idato] = datos_pelacus['Prof_teor.'][idato]        
        if datos_pelacus['Prof_real'][idato] is None and datos_pelacus['Prof_teor.'][idato] is None:
            datos_pelacus['profundidad'][idato] = -999    
   
    datos_pelacus = datos_pelacus.drop(columns=['Prof_est','Prof_real', 'Prof_teor.'])

    # Renombra las columnas para mantener una denominación homogénea
    datos_pelacus = datos_pelacus.rename(columns={"campaña":"programa","cast":"nombre_muestreo","fecha":"fecha_muestreo","hora":"hora_muestreo","estación":"estacion",
                                                  "Latitud":"latitud","Longitud":"longitud","t_CTD":"temperatura_ctd","Sal_CTD":"salinidad_ctd","SiO2":"sio4","SiO2_flag":"sio4_qf",
                                                  "NO3":"no3","NO3T_flag":"no3_qf","NO2":"no2","NO2_flag":"no2_qf","NH4":"nh4","NH4_flag":"nh4_qf","PO4":"po4","PO4_flag":"po4_qf","Cla":"clorofila_a"
                                                  })

    return datos_pelacus






##########################################################################
######## FUNCION PARA APLICAR CONTROL DE CALIDA BÁSICO LOS DATOS  ########
##########################################################################
def control_calidad(datos,archivo_variables_base_datos):
 
    # Carga información de las variables utilizadas en la base de datos  
    datos_general     = pandas.read_excel(archivo_variables_base_datos, 'variables')    
    
    # Lee las variables de cada tipo a utilizar en el control de calidad
    variables_muestreo = [x for x in datos_general['parametros_muestreo'] if str(x) != 'nan']
    variables_fisicas  = [x for x in datos_general['variables_fisicas'] if str(x) != 'nan']    
    variables_biogeoquimicas  = [x for x in datos_general['variables_biogeoquimicas'] if str(x) != 'nan'] 
    
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
    
    # Reordena las columnas del dataframe para que tengan el mismo orden que el listado de variables
    datos = datos.reindex(columns=listado_completo)
    
    datos = datos.replace({numpy.nan:None})
    
    # Eliminar los registros sin dato de latitud,longitud, profundidad o fecha 
    datos = datos[datos['latitud'].notna()]
    datos = datos[datos['longitud'].notna()]  
    datos = datos[datos['profundidad'].notna()] 
    datos = datos[datos['fecha_muestreo'].notna()] 
    
    # Elimina los registros con datos de profundidad negativos
    datos = datos.drop(datos[datos.profundidad < 0].index)
     
    # Corregir los valores positivos de longitud, pasándolos a negativos (algunos datos de Pelacus tienen este error)
    datos['longitud'] = -1*datos['longitud'].abs()  
    

    #datos = datos.round({"longitud": 4, "latitud": 4})
    
    # Define un nuevo índice de filas. Si se han eliminado registros este paso es necesario
    indices_dataframe        = numpy.arange(0,datos.shape[0],1,dtype=int)    
    datos['id_temp'] = indices_dataframe
    datos.set_index('id_temp',drop=False,append=False,inplace=True)
    
    # Redondea a 4 decimales los datos de latitud y longitud (precisión utilizada en la base de datos)
    for idato in range(datos.shape[0]):
        datos['longitud'][idato] = round(datos['longitud'][idato],4)
        datos['latitud'][idato] = round(datos['latitud'][idato],4)
    
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

    return datos    
   





##############################################################################
######## FUNCION PARA ENCONTRAR LA ESTACIÓN ASOCIADA A CADA REGISTRO  ########
##############################################################################

def evalua_estaciones(datos,min_dist,id_programa,direccion_host,base_datos,usuario,contrasena,puerto):

    
    for idato in range(datos.shape[0]):
    
        datos_insercion = (str(datos['estacion'][idato]),round(datos['latitud'][idato],4),round(datos['longitud'][idato],4),int(id_programa))
            
        instruccion_sql = "SELECT id_estacion FROM estaciones WHERE nombre_estacion = %s AND latitud = %s AND longitud=%s AND programa = %s;"
        cursor.execute(instruccion_sql, datos_insercion)
        datos_temporal = cursor.fetchone()
        conn.commit()
        
        if datos_temporal is None:
            
            instruccion_sql = "INSERT INTO estaciones (nombre_estacion,latitud,longitud,programa) VALUES (%s,%s,%s,%s);"   
            cursor.execute(instruccion_sql, (datos_insercion))
            conn.commit() 
            
            instruccion_sql = "SELECT id_estacion FROM estaciones WHERE nombre_estacion = %s AND latitud = %s AND longitud=%s AND programa = %s;"
            cursor.execute(instruccion_sql, datos_insercion)
            datos_temporal = cursor.fetchone()
            conn.commit()
            
        datos['id_estacion_temp'][idato] = datos_temporal[0]    
         
            

    # # recupera las estaciones disponibles en la base de datos
    # #conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)

    # con_engine = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    # conn       = create_engine(con_engine)
    # tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
    # #conn.close() 
    
    # # Recorta el dataframe para tener sólo las estaciones del programa seleccionado
    # estaciones_programa            = tabla_estaciones[tabla_estaciones['programa'] == id_programa]
    # indices_dataframe              = numpy.arange(1,estaciones_programa.shape[0]+1,1,dtype=int)    
    # estaciones_programa['id_temp'] = indices_dataframe
    # estaciones_programa.set_index('id_temp',drop=True,append=False,inplace=True)
        
    
    # ## Identifica la estación asociada a cada registro
    
    # # Columna para punteros de estaciones
    # datos['id_estacion_temp'] = numpy.zeros(datos.shape[0],dtype=int) 
    
    # # Genera un dataframe con la combinación única de latitud/longitud en los muestreos
    # coordenadas_unicas            = datos.groupby(['latitud','longitud']).size().reset_index().rename(columns={0:'count'})
    # coordenadas_unicas['identif'] = numpy.arange(0,coordenadas_unicas.shape[0],dtype=int)
    
    # # Redondea a la misma precisión que las coordenadas almacenadas en la base de datos
    # coordenadas_unicas = coordenadas_unicas.round({'longitud': 4, 'latitud': 4})
    
    # # Busca los registros con la misma lat/lon que cada par de coordenadas únicas, a los que asigna un identificado (nº de estación)
    # for idato in range(coordenadas_unicas.shape[0]):
    #     aux = (datos['latitud'] == coordenadas_unicas['latitud'][idato]) & (datos['longitud'] == coordenadas_unicas['longitud'][idato])
    #     indices = [i for i, x in enumerate(aux) if x]
    #     datos['id_estacion_temp'][indices] = coordenadas_unicas['identif'][idato]

    # return datos    


    # # Recupera las estaciones disponibles en la base de datos
    # #conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)   
    # con_engine = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    # engine = create_engine(con_engine)
    # tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', engine)
    
    # # Recorta el dataframe para tener sólo las estaciones del programa seleccionado
    # datos_estaciones = tabla_estaciones[tabla_estaciones['programa'] == id_programa]
    # indices_dataframe        = numpy.arange(0,datos_estaciones.shape[0],1,dtype=int)    
    # datos_estaciones['id_temp'] = indices_dataframe
    # datos_estaciones.set_index('id_temp',drop=True,append=False,inplace=True)
        
    
    # # identifica la estación asociada a cada registro
    # datos['id_estacion_temp'] = numpy.zeros(datos.shape[0],dtype=int) 
    # proy_datos                = Proj(proj='utm',zone=29,ellps='WGS84', preserve_units=False) # Referencia coords
    
    # for iregistro in range(datos.shape[0]): 
    #     if datos_estaciones.shape[0] == 0:
                     
    #         nueva_estacion = {'id_estacion':1,'nombre_estacion':datos['estacion'][iregistro], 'latitud':datos['latitud'][iregistro], 'longitud':datos['longitud'][iregistro], 'programa':id_programa}
    # #        datos_estaciones = datos_estaciones.concat(nueva_estacion, ignore_index=True)
    #         datos_estaciones.loc[1] = nueva_estacion       
    
    #     else:
    #         vector_distancias      = numpy.zeros(datos_estaciones.shape[0])
    #         vector_identificadores = numpy.zeros(datos_estaciones.shape[0],dtype=int)
           
    #         # Determina la distancia de cada registro a las estaciones incluidas en la base de datos
    #         for iestacion_disponible in range(datos_estaciones.shape[0]):
    #             x_muestreo, y_muestreo = proy_datos(datos['longitud'][iregistro], datos['latitud'][iregistro], inverse=False)
    #             x_bd, y_bd             = proy_datos(datos_estaciones['longitud'][iestacion_disponible], datos_estaciones['latitud'][iestacion_disponible], inverse=False)
    #             distancia              = math.sqrt((((x_muestreo-x_bd)**2) + ((y_muestreo-y_bd)**2)))
               
    #             vector_distancias[iestacion_disponible]      = distancia
    #             vector_identificadores[iestacion_disponible] = int(datos_estaciones['id_estacion'][iestacion_disponible])
               
    #         # Si la distancia a alguna de las estaciones es menor a la distancia mínima, la estación ya está en la base de datos
    #         if min(vector_distancias) <= min_dist :
    #             ipos_minimo = numpy.argmin(vector_distancias)
    #             datos['id_estacion_temp'][iregistro] = vector_identificadores[ipos_minimo]
               
    #         # En caso contrario, la estación es nueva y se añade a la base de datos
    #         else:
    #             indice_insercion = int(max(datos_estaciones['id_estacion']) + 1)
     
    #             nueva_estacion = {'id_estacion':indice_insercion,'nombre_estacion':datos['estacion'][iregistro], 'latitud':datos['latitud'][iregistro], 'longitud':datos['longitud'][iregistro], 'programa':id_programa}
    #             #datos_estaciones = datos_estaciones.concat(nueva_estacion, ignore_index=True)
    #             datos_estaciones.loc[datos_estaciones.shape[0]] = nueva_estacion 
             
    #             datos['id_estacion_temp'][iregistro] = indice_insercion 
    
    
    # # Inserta las nuevas estaciones
    # conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    # cursor = conn.cursor()
    
    # for iestacion in range(datos_estaciones.shape[0]):
    #     datos_insercion = (int(datos_estaciones['id_estacion'][iestacion]),str(datos_estaciones['nombre_estacion'][iestacion]),round(datos_estaciones['latitud'][iestacion],4),round(datos_estaciones['longitud'][iestacion],4),int(id_programa))
    #     instruccion_sql = "INSERT INTO estaciones (id_estacion,nombre_estacion,latitud,longitud,programa) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (id_estacion) DO NOTHING;"   
    #     cursor.execute(instruccion_sql, (datos_insercion))
    #     conn.commit() 
    # cursor.close()
    # conn.close()
    
    # return datos



###########################################################################
######## FUNCION PARA ENCONTRAR EL IDENTIFICADOR DE CADA REGISTRO  ########
###########################################################################

def evalua_registros(datos,direccion_host,base_datos,usuario,contrasena,puerto):
    
    ### DETERMINA EL NUMERO DE REGISTRO DE CADA MUESTREO 
    
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()
    
    datos['id_muestreo_temp'] = numpy.zeros(datos.shape[0],dtype=int) 
     
    
    for idato in range(datos.shape[0]):
    
        nombre_muestreo = nombre_programa + '_' + str(datos['fecha_muestreo'][idato].year) + '_E' + str(datos['id_estacion_temp'][idato])
    
        # Por seguridad, convierte el identificador de botella a entero si éste existe
        if datos['botella'][idato] is not None:
            id_botella = int(datos['botella'][idato])
        else:
            id_botella = None
            
        # Por incompatibilidad con POSTGRESQL hay que "desmontar" y volver a montar las fechas
        anho           = datos['fecha_muestreo'][idato].year # 
        mes            = datos['fecha_muestreo'][idato].month
        dia            = datos['fecha_muestreo'][idato].day
        fecha_consulta = datetime.date(anho,mes,dia) 
        
        # Intenta insertar el muestreo correspondiente al registro. Si ya existe en la base de datos no hará nada, de lo contrario añadirá el nuevo muestreo
        # Distinta instrucción según haya información de hora o no (para hacer el script más tolerante a fallos)
        
        if datos['hora_muestreo'][idato] is not None:
            # Si es un string conviertelo a time
            if isinstance(datos['hora_muestreo'][idato], str) is True:
                hora_temporal = datetime.datetime.strptime(datos['hora_muestreo'][idato],'%H:%M')
            # Si es un datetime conviertelo a time
            elif isinstance(datos['hora_muestreo'][idato], datetime.datetime) is True:
                hora_temporal = datos['hora_muestreo'][idato].time()
            else:
                hora_temporal= datos['hora_muestreo'][idato]
            
            # Por incompatibilidad con POSTGRESQL también hay que "desmontar" y volver a montar las horas
            hora          = hora_temporal.hour
            minuto        = hora_temporal.minute
            hora_consulta = datetime.time(hora,minuto) 
                
            instruccion_sql = "INSERT INTO muestreos_discretos (nombre_muestreo,estacion,fecha_muestreo,hora_muestreo,profundidad,botella,configuracion_perfilador,configuracion_superficie) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_muestreo,profundidad,configuracion_perfilador,configuracion_superficie) DO NOTHING;"   
            cursor.execute(instruccion_sql, (nombre_muestreo,int(datos['id_estacion_temp'][idato]),fecha_consulta,hora_consulta,round(datos['profundidad'][idato],2),id_botella,int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            conn.commit()
     
            instruccion_sql = "SELECT id_muestreo FROM muestreos_discretos WHERE estacion = %s AND fecha_muestreo = %s AND hora_muestreo=%s AND profundidad = %s AND configuracion_perfilador = %s AND configuracion_superficie = %s;"
            cursor.execute(instruccion_sql, (int(datos['id_estacion_temp'][idato]),fecha_consulta,datos['hora_muestreo'][idato],round(datos['profundidad'][idato],2),int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            id_muestreos_bd =cursor.fetchone()
            conn.commit()     
        
    
        else:
            
            instruccion_sql = "INSERT INTO muestreos_discretos (nombre_muestreo,estacion,fecha_muestreo,profundidad,botella,configuracion_perfilador,configuracion_superficie) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_muestreo,profundidad,configuracion_perfilador,configuracion_superficie) DO NOTHING;"   
            cursor.execute(instruccion_sql, (nombre_muestreo,int(datos['id_estacion_temp'][idato]),fecha_consulta,round(datos['profundidad'][idato],2),id_botella,int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            conn.commit()
                
            instruccion_sql = "SELECT id_muestreo FROM muestreos_discretos WHERE estacion = %s AND fecha_muestreo = %s AND profundidad = %s AND configuracion_perfilador = %s AND configuracion_superficie = %s;"
            cursor.execute(instruccion_sql, (int(datos['id_estacion_temp'][idato]),fecha_consulta,round(datos['profundidad'][idato],2),int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            id_muestreos_bd =cursor.fetchone()
            conn.commit() 
        
        datos['id_muestreo_temp'][idato] =  id_muestreos_bd[0]
    
    cursor.close()
    conn.close() 

    return datos














































#####################################################################################
######## FUNCION PARA INSERTAR LOS DATOS, YA AJUSTADOS, EN LA BASE DE DATOS  ########
#####################################################################################


def inserta_datos(datos,min_dist,nombre_programa,id_programa,direccion_host,base_datos,usuario,contrasena,puerto):
 
    
 
    ### IDENTIFICA LAS ESTACIONES MUESTREADAS Y EVALUA SI YA EXISTEN EN LA BASE DE DATOS (TABLA ESTACIONES)
    
    datos['id_estacion_temp'] = numpy.zeros(datos.shape[0],dtype=int) 
    
    proy_datos                = Proj(proj='utm',zone=29,ellps='WGS84', preserve_units=False) # Referencia coords
    
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()
    
    for iregistro in range(datos.shape[0]):  
    
        # Busca el identificador de la estacion
        instruccion_sql = 'SELECT id_estacion,longitud,latitud FROM estaciones WHERE programa = %s ;'
        cursor.execute(instruccion_sql,(str(id_programa)))
        estaciones_disponibles =cursor.fetchall()
        conn.commit()     
        
        # Si no hay registros de estaciones en la bas de datos, insertar la estación muestreada directamente
        if len(estaciones_disponibles) == 0:
            datos_insercion = (str(datos['estacion'][iregistro]),round(datos['latitud'][iregistro],4),round(datos['longitud'][iregistro],4),int(id_programa))
            instruccion_sql = "INSERT INTO estaciones (nombre_estacion,latitud,longitud,programa) VALUES (%s,%s,%s,%s) ON CONFLICT (id_estacion) DO NOTHING;"   
            cursor.execute(instruccion_sql, (datos_insercion))
            conn.commit() 
    
            datos['id_estacion_temp'][iregistro] = 1
        
        # En caso contrario, buscar si hay una estación en el mismo punto (+- min_dist)
        else:
            
            vector_distancias      = numpy.zeros(len(estaciones_disponibles))
            vector_identificadores = numpy.zeros(len(estaciones_disponibles),dtype=int)
            
            # Determina la distancia de cada registro a las estaciones incluidas en la base de datos
            for iestacion_disponible in range(len(estaciones_disponibles)):
                x_muestreo, y_muestreo = proy_datos(datos['longitud'][iregistro], datos['latitud'][iregistro], inverse=False)
                x_bd, y_bd             = proy_datos(float(estaciones_disponibles[iestacion_disponible][1]), float(float(estaciones_disponibles[iestacion_disponible][2])), inverse=False)
                distancia              = math.sqrt((((x_muestreo-x_bd)**2) + ((y_muestreo-y_bd)**2)))
                
                vector_distancias[iestacion_disponible]      = distancia
                vector_identificadores[iestacion_disponible] = int(estaciones_disponibles[iestacion_disponible][0])
                
            # Si la distancia a alguna de las estaciones es menor a la distancia mínima, la estación ya está en la base de datos
            if min(vector_distancias) <= min_dist :
                ipos_minimo = numpy.argmin(vector_distancias)
                datos['id_estacion_temp'][iregistro] = int(estaciones_disponibles[ipos_minimo][0])
                
            # En caso contrario, la estación es nueva y se añade a la base de datos
            else:
                indice_insercion = max(vector_identificadores) + 1
                datos_insercion = (str(datos['estacion'][iregistro]),round(datos['latitud'][iregistro],4),round(datos['longitud'][iregistro],4),int(id_programa))
                instruccion_sql = "INSERT INTO estaciones (nombre_estacion,latitud,longitud,programa) VALUES (%s,%s,%s,%s) ON CONFLICT (id_estacion) DO NOTHING;"   
                cursor.execute(instruccion_sql, (datos_insercion))
                conn.commit() 
        
                datos['id_estacion_temp'][iregistro] = indice_insercion
                
    cursor.close()
    conn.close()
    
     
    ### DETERMINA EL NUMERO DE REGISTRO DE CADA MUESTREO 
    
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()
    
    datos['id_muestreo_temp'] = numpy.zeros(datos.shape[0],dtype=int) 
     
    
    for idato in range(datos.shape[0]):
    #for idato in range(300): 
    
        nombre_muestreo = nombre_programa + '_' + str(datos['fecha_muestreo'][idato].year) + '_E' + str(datos['id_estacion_temp'][idato])
    
        # Por seguridad, convierte el identificador de botella a entero si éste existe
        if datos['botella'][idato] is not None:
            id_botella = int(datos['botella'][idato])
        else:
            id_botella = None
            
        # Por incompatibilidad con POSTGRESQL hay que "desmontar" y volver a montar las fechas
        anho           = datos['fecha_muestreo'][idato].year # 
        mes            = datos['fecha_muestreo'][idato].month
        dia            = datos['fecha_muestreo'][idato].day
        fecha_consulta = datetime.date(anho,mes,dia) 
        
        # Intenta insertar el muestreo correspondiente al registro. Si ya existe en la base de datos no hará nada, de lo contrario añadirá el nuevo muestreo
        # Distinta instrucción según haya información de hora o no (para hacer el script más tolerante a fallos)
        
        if datos['hora_muestreo'][idato] is not None:
            # Si es un string conviertelo a time
            if isinstance(datos['hora_muestreo'][idato], str) is True:
                hora_temporal = datetime.datetime.strptime(datos['hora_muestreo'][idato],'%H:%M')
            # Si es un datetime conviertelo a time
            elif isinstance(datos['hora_muestreo'][idato], datetime.datetime) is True:
                hora_temporal = datos['hora_muestreo'][idato].time()
            else:
                hora_temporal= datos['hora_muestreo'][idato]
            
            # Por incompatibilidad con POSTGRESQL también hay que "desmontar" y volver a montar las horas
            hora          = hora_temporal.hour
            minuto        = hora_temporal.minute
            hora_consulta = datetime.time(hora,minuto) 
                
            instruccion_sql = "INSERT INTO muestreos_discretos (nombre_muestreo,estacion,fecha_muestreo,hora_muestreo,profundidad,botella,configuracion_perfilador,configuracion_superficie) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_muestreo,profundidad,configuracion_perfilador,configuracion_superficie) DO NOTHING;"   
            cursor.execute(instruccion_sql, (nombre_muestreo,int(datos['id_estacion_temp'][idato]),fecha_consulta,hora_consulta,round(datos['profundidad'][idato],2),id_botella,int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            conn.commit()
     
            instruccion_sql = "SELECT id_muestreo FROM muestreos_discretos WHERE estacion = %s AND fecha_muestreo = %s AND hora_muestreo=%s AND profundidad = %s AND configuracion_perfilador = %s AND configuracion_superficie = %s;"
            cursor.execute(instruccion_sql, (int(datos['id_estacion_temp'][idato]),fecha_consulta,datos['hora_muestreo'][idato],round(datos['profundidad'][idato],2),int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            id_muestreos_bd =cursor.fetchone()
            conn.commit()     
        
    
        else:
            
            instruccion_sql = "INSERT INTO muestreos_discretos (nombre_muestreo,estacion,fecha_muestreo,profundidad,botella,configuracion_perfilador,configuracion_superficie) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_muestreo,profundidad,configuracion_perfilador,configuracion_superficie) DO NOTHING;"   
            cursor.execute(instruccion_sql, (nombre_muestreo,int(datos['id_estacion_temp'][idato]),fecha_consulta,round(datos['profundidad'][idato],2),id_botella,int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            conn.commit()
                
            instruccion_sql = "SELECT id_muestreo FROM muestreos_discretos WHERE estacion = %s AND fecha_muestreo = %s AND profundidad = %s AND configuracion_perfilador = %s AND configuracion_superficie = %s;"
            cursor.execute(instruccion_sql, (int(datos['id_estacion_temp'][idato]),fecha_consulta,round(datos['profundidad'][idato],2),int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            id_muestreos_bd =cursor.fetchone()
            conn.commit() 
        
        datos['id_muestreo_temp'][idato] =  id_muestreos_bd[0]
    
    cursor.close()
    conn.close() 
    
    
    
    
    
    
    ### INSERTA LOS DATOS EN LA TABLA DE MUESTREOS PUNTUALES FISICA
    
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()
    
    for idato in range(datos.shape[0]):
        
        # Intenta insertar los muestreos. Si ya existen no hará nada, de lo contrario añade el registro
        datos_insercion = (
            int(datos['id_muestreo_temp'][idato]),
            datos['temperatura_ctd'][idato],datos['temperatura_ctd_qf'][idato],
            datos['salinidad_ctd'][idato],datos['salinidad_ctd_qf'][idato],
            datos['par_ctd'][idato],datos['par_ctd_qf'][idato]   
            )
     
        parametros = ['muestreo',
                      'temperatura_ctd','temperatura_ctd_qf',
                      'salinidad_ctd','salinidad_ctd_qf',
                      'par_ctd','par_ctd_qf'
                      ]
     
        str_variables =  'VALUES ('
        str_parametros = '('
        for iparametro in range(len(parametros)-1):
            str_variables  = str_variables + '%s,'
            str_parametros = str_parametros + parametros[iparametro] + ','
        str_variables = str_variables + '%s)'    
        str_parametros = str_parametros + parametros[-1] + ')'
        
        
        str_actualiza  = 'DO UPDATE SET ('
        for iparametro in range(len(parametros)-2):
            str_actualiza = str_actualiza + parametros[iparametro+1] + ',' 
        str_actualiza = str_actualiza + parametros[-1] + ') = ('
        for iparametro in range(len(parametros)-2):
            str_actualiza = str_actualiza + 'EXCLUDED.' +  parametros[iparametro+1] + ',' 
        str_actualiza = str_actualiza + 'EXCLUDED.' + parametros[-1] + ')' 
        
        instruccion_sql = "INSERT INTO datos_discretos_fisica " + str_parametros + " " + str_variables + "ON CONFLICT (muestreo) " + str_actualiza + ";"       
        cursor.execute(instruccion_sql,datos_insercion)
        conn.commit()
        
    cursor.close()
    conn.close()
    
    
    
    
    ### INSERTA LOS DATOS EN LA TABLA DE MUESTREOS PUNTUALES BIOGEOQUIMICOS
    
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()
    
    for idato in range(datos.shape[0]):
    
        # Intenta insertar los muestreos. Si ya existe el muestreo, actualiza las variables biogeoquímicas con los datos cargados. 
        # De lo contrario, añade el registro correspondiente al nuevo muestreo
    
        datos_insercion = (int(datos['id_muestreo_temp'][idato]),
            datos['fluorescencia_ctd'][idato],datos['fluorescencia_ctd_qf'][idato],
            datos['oxigeno_ctd'][idato],datos['oxigeno_ctd_qf'][idato],
            datos['oxigeno_wk'][idato],datos['oxigeno_wk_qf'][idato],
            datos['sio4'][idato],datos['sio4_qf'][idato],
            datos['no3'][idato],datos['no3_qf'][idato],
            datos['no2'][idato],datos['no2_qf'][idato],
            datos['po4'][idato],datos['po4_qf'][idato],
            datos['tcarbn'][idato],datos['tcarbn_qf'][idato],
            datos['alkali'][idato],datos['alkali_qf'][idato],
            datos['phts25P0_unpur'][idato],datos['phts25P0_unpur_qf'][idato],
            datos['phts25P0_pur'][idato],datos['phts25P0_pur_qf'][idato],
            datos['r_clor'][idato],datos['r_clor_qf'][idato],
            datos['r_per'][idato],datos['r_per_qf'][idato],
            datos['co3_temp'][idato]        
            )
      
        parametros = ['muestreo',
        'fluorescencia_ctd','fluorescencia_ctd_qf',
        'oxigeno_ctd','oxigeno_ctd_qf',
        'oxigeno_wk','oxigeno_wk_qf',
        'sio4','sio4_qf',
        'no3','no3_qf',
        'no2','no2_qf',
        'po4','po4_qf',
        'tcarbn','tcarbn_qf',
        'alkali','alkali_qf',
        'phts25P0_unpur',' phts25P0_unpur_qf',
        'phts25P0_pur','phts25P0_pur_qf',
        'r_clor','r_clor_qf',
        'r_per','r_per_qf',
        'co3_temp']
        
        str_variables =  'VALUES ('
        str_parametros = '('
        for iparametro in range(len(parametros)-1):
            str_variables  = str_variables + '%s,'
            str_parametros = str_parametros + parametros[iparametro] + ','
        str_variables = str_variables + '%s)'    
        str_parametros = str_parametros + parametros[-1] + ')'
        
        
        str_actualiza  = 'DO UPDATE SET ('
        for iparametro in range(len(parametros)-2):
            str_actualiza = str_actualiza + parametros[iparametro+1] + ',' 
        str_actualiza = str_actualiza + parametros[-1] + ') = ('
        for iparametro in range(len(parametros)-2):
            str_actualiza = str_actualiza + 'EXCLUDED.' +  parametros[iparametro+1] + ',' 
        str_actualiza = str_actualiza + 'EXCLUDED.' + parametros[-1] + ')' 
        
        instruccion_sql = "INSERT INTO datos_discretos_biogeoquimica " + str_parametros + " " + str_variables + "ON CONFLICT (muestreo) " + str_actualiza + ";"       
        cursor.execute(instruccion_sql,datos_insercion)
        conn.commit()
    
    cursor.close()
    conn.close()
        
 
     
 
    
 
    
    
    
 
###################################################################################################
######## FUNCION PARA ACTUALIZAR LA TABLA DE ESTADOS, UTILIZADA POR LA APLICACIO STREAMLIT ########
###################################################################################################
    
def actualiza_estado(datos,id_programa,nombre_programa,itipo_informacion,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto):

    # Define la fecha actual
    fecha_actual = datetime.date.today()
    
    # Busca de cuántos años diferentes contiene información el dataframe
    vector_auxiliar_tiempo = numpy.zeros(datos.shape[0],dtype=int)
    for idato in range(datos.shape[0]):
        vector_auxiliar_tiempo[idato] = datos['fecha_muestreo'][idato].year
    anhos_muestreados                 = numpy.unique(vector_auxiliar_tiempo)
    datos['año']                      = vector_auxiliar_tiempo 
    
    # Procesado para cada uno de los años incluidos en el dataframe importado
    for ianho in range(len(anhos_muestreados)):
        
        anho_procesado = anhos_muestreados[ianho]
        
        #anho_procesado = 2045
        
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
            datos_bd = [None]*8
                
            # Genera el vector con los datos a insertar. diferente según sea análisis o post-procesado
            if itipo_informacion == 1: # La información a insertar es un nuevo registro de análisis de laboratorio
                datos_insercion     = [int(id_programa),nombre_programa,int(anho_procesado),fecha_final_muestreo,fecha_actual,None,email_contacto,None]
            
            if itipo_informacion == 2: # La información a insertar es un registro de post-procesado 
                datos_insercion     = [int(id_programa),nombre_programa,int(anho_procesado),fecha_final_muestreo,None,fecha_actual,None,email_contacto]
            
            # Inserta la información en la base de datos
            instruccion_sql = "INSERT INTO estado_procesos (programa,nombre_programa,año,fecha_final_muestreo,fecha_analisis_laboratorio,fecha_post_procesado,contacto_muestreo,contacto_post_procesado) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,año) DO UPDATE SET (nombre_programa,fecha_final_muestreo,fecha_analisis_laboratorio,fecha_post_procesado,contacto_muestreo,contacto_post_procesado) = (EXCLUDED.nombre_programa,EXCLUDED.fecha_final_muestreo,EXCLUDED.fecha_analisis_laboratorio,EXCLUDED.fecha_post_procesado,EXCLUDED.contacto_muestreo,EXCLUDED.contacto_post_procesado);"   
            cursor.execute(instruccion_sql, (datos_insercion))
            conn.commit()

        # Si la base de datos ya contiene registros del programa y año a insertar, actualizar las fechas correspondientes
        else:
            if itipo_informacion == 1:
                instruccion_sql = "UPDATE estado_procesos SET fecha_analisis_laboratorio = %s,contacto_muestreo = %s WHERE programa = %s AND año = %s;"   
                cursor.execute(instruccion_sql, (fecha_actual,email_contacto,int(id_programa),int(anho_procesado)))
                conn.commit()                

            if itipo_informacion == 2:
                instruccion_sql = "UPDATE estado_procesos SET fecha_post_procesado = %s,contacto_post_procesado = %s WHERE programa = %s AND año = %s;"   
                cursor.execute(instruccion_sql, (fecha_actual,email_contacto,int(id_programa),int(anho_procesado)))
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
    
    # Identificador del programa (RADIAL CORUNA en este caso)
    instruccion_sql = "SELECT id_programa FROM programas WHERE nombre_programa = '" + nombre_programa + "';"
    cursor.execute(instruccion_sql)
    id_programa =cursor.fetchone()[0]
    conn.commit()
    
    cursor.close()
    conn.close()    

    return id_programa

















# base_datos     = 'COAC'
# usuario        = 'postgres'
# contrasena     = 'm0nt34lt0'
# puerto         = '5432'
# direccion_host = '193.146.155.99'




# listado_variables = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/VARIABLES.xlsx'

    
  
# nombre_archivo = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/RADIAL_BTL_COR_2015.xlsx'   
# datos_radiales = lectura_datos_radiales(nombre_archivo,direccion_host,base_datos,usuario,contrasena,puerto) 

# nombre_archivo = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx'   
# datos_pelacus = lectura_datos_pelacus(nombre_archivo)    

# datos = control_calidad(datos_pelacus,listado_variables)  
# # id_programa = 3
# # min_dist = 50
# # nombre_programa = "RADIAL CORUÑA"

# id_programa = 1
# min_dist = 50
# nombre_programa = "PELACUS"

# print(datetime.datetime.now())



# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# datos['id_estacion_temp']  = numpy.zeros(datos.shape[0],dtype=int)

# for idato in range(datos.shape[0]):

#     datos_insercion = (str(datos['estacion'][idato]),round(datos['latitud'][idato],4),round(datos['longitud'][idato],4),int(id_programa))
        
#     instruccion_sql = "SELECT id_estacion FROM estaciones WHERE nombre_estacion = %s AND latitud = %s AND longitud=%s AND programa = %s;"
#     cursor.execute(instruccion_sql, datos_insercion)
#     datos_temporal = cursor.fetchone()
#     conn.commit()
    
#     if datos_temporal is None:
        
#         instruccion_sql = "INSERT INTO estaciones (nombre_estacion,latitud,longitud,programa) VALUES (%s,%s,%s,%s);"   
#         cursor.execute(instruccion_sql, (datos_insercion))
#         conn.commit() 
        
#         instruccion_sql = "SELECT id_estacion FROM estaciones WHERE nombre_estacion = %s AND latitud = %s AND longitud=%s AND programa = %s;"
#         cursor.execute(instruccion_sql, datos_insercion)
#         datos_temporal = cursor.fetchone()
#         conn.commit()
        
#     datos['id_estacion_temp'][idato] = datos_temporal[0]    
     
        

# print(datetime.datetime.now())

     # str_actualiza  = 'DO UPDATE SET ('
     # for iparametro in range(len(parametros)-2):
     #     str_actualiza = str_actualiza + parametros[iparametro+1] + ',' 
     # str_actualiza = str_actualiza + parametros[-1] + ') = ('
     # for iparametro in range(len(parametros)-2):
     #     str_actualiza = str_actualiza + 'EXCLUDED.' +  parametros[iparametro+1] + ',' 
     # str_actualiza = str_actualiza + 'EXCLUDED.' + parametros[-1] + ')' 




#         datos_insercion = (str(datos_estaciones['nombre_estacion'][iestacion]),round(coordenadas_unicas['latitud'][iestacion],4),round(coordenadas_unicas['longitud'][iestacion],4),int(id_programa))
#         instruccion_sql = "INSERT INTO estaciones (nombre_estacion,latitud,longitud,programa) VALUES (%s,%s,%s,%s) ON CONFLICT (id_estacion) DO NOTHING;"   
#         cursor.execute(instruccion_sql, (datos_insercion))
#         conn.commit() 















# con_engine = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn       = create_engine(con_engine)
# tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
 

# # Recorta el dataframe para tener sólo las estaciones del programa seleccionado
# estaciones_programa            = tabla_estaciones[tabla_estaciones['programa'] == id_programa]
# indices_dataframe              = numpy.arange(0,estaciones_programa.shape[0],1,dtype=int)    
# estaciones_programa['id_temp'] = indices_dataframe
# estaciones_programa.set_index('id_temp',drop=True,append=False,inplace=True)

# ## Identifica la estación asociada a cada registro

# # Columna para punteros de estaciones
# datos['id_estacion_temp'] = numpy.zeros(datos.shape[0],dtype=int) 

# # Genera un dataframe con la combinación única de latitud/longitud en los muestreos
# coordenadas_unicas             = datos.groupby(['latitud','longitud']).size().reset_index().rename(columns={0:'count'})
# coordenadas_unicas['identif']  = numpy.zeros(coordenadas_unicas.shape[0],dtype=int)
# coordenadas_unicas['io_nueva'] = numpy.zeros(coordenadas_unicas.shape[0],dtype=int)

# # si no hay estaciones de un programa determinado, insertar todas las estaciones
# if estaciones_programa.shape[0]==0:
 
#     conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
#     cursor = conn.cursor()
#     for idato in range(coordenadas_unicas.shape[0]):
        
#         datos_insercion = (str(datos_estaciones['nombre_estacion'][iestacion]),round(coordenadas_unicas['latitud'][iestacion],4),round(coordenadas_unicas['longitud'][iestacion],4),int(id_programa))
#         instruccion_sql = "INSERT INTO estaciones (nombre_estacion,latitud,longitud,programa) VALUES (%s,%s,%s,%s) ON CONFLICT (id_estacion) DO NOTHING;"   
#         cursor.execute(instruccion_sql, (datos_insercion))
#         conn.commit() 

# # Redondea a la misma precisión que las coordenadas almacenadas en la base de datos
# coordenadas_unicas = coordenadas_unicas.round({'longitud': 4, 'latitud': 4})

# # Asocia a cada par de coordenadas unicas, la estación correspondiente
# for idato in range(estaciones_programa.shape[0]):
#     aux = (coordenadas_unicas ['latitud'] == estaciones_programa['latitud'][idato]) & (coordenadas_unicas ['longitud'] == estaciones_programa['longitud'][idato])
#     if any(aux) is True:
#         indices = [i for i, x in enumerate(aux) if x]
#         coordenadas_unicas['identif'][indices]  = estaciones_programa['id_estacion'][idato]
#         coordenadas_unicas['io_nueva'][indices] = 1
        
# # Extrae las estaciones nuevas (io_nueva = 1)        
# coordenadas_unicas[coordenadas_unicas['io_nueva']==1] = numpy.arange(max(tabla_estaciones['id_estacion']))   
        
        

# # Busca los registros con la misma lat/lon que cada par de coordenadas únicas, a los que asigna un identificado (nº de estación)
# for idato in range(coordenadas_unicas.shape[0]):

#     aux = (datos['latitud'] == coordenadas_unicas['latitud'][idato]) & (datos['longitud'] == coordenadas_unicas['longitud'][idato])
#     indices = [i for i, x in enumerate(aux) if x]
#     datos['id_estacion_temp'][indices] = coordenadas_unicas['identif'][idato]

# print(datetime.datetime.now())




# ### DETERMINA EL NUMERO DE REGISTRO DE CADA MUESTREO 

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()



# datos['id_muestreo_temp'] = numpy.zeros(datos.shape[0],dtype=int) 
 

# for idato in range(datos.shape[0]):

#     nombre_muestreo = nombre_programa + '_' + str(datos['fecha_muestreo'][idato].year) + '_E' + str(datos['id_estacion_temp'][idato])

#     # Por seguridad, convierte el identificador de botella a entero si éste existe
#     if datos['botella'][idato] is not None:
#         id_botella = int(datos['botella'][idato])
#     else:
#         id_botella = None
        
#     # Por incompatibilidad con POSTGRESQL hay que "desmontar" y volver a montar las fechas
#     anho           = datos['fecha_muestreo'][idato].year # 
#     mes            = datos['fecha_muestreo'][idato].month
#     dia            = datos['fecha_muestreo'][idato].day
#     fecha_consulta = datetime.date(anho,mes,dia) 
    
#     # Intenta insertar el muestreo correspondiente al registro. Si ya existe en la base de datos no hará nada, de lo contrario añadirá el nuevo muestreo
#     # Distinta instrucción según haya información de hora o no (para hacer el script más tolerante a fallos)
    
#     if datos['hora_muestreo'][idato] is not None:
#         # Si es un string conviertelo a time
#         if isinstance(datos['hora_muestreo'][idato], str) is True:
#             hora_temporal = datetime.datetime.strptime(datos['hora_muestreo'][idato],'%H:%M')
#         # Si es un datetime conviertelo a time
#         elif isinstance(datos['hora_muestreo'][idato], datetime.datetime) is True:
#             hora_temporal = datos['hora_muestreo'][idato].time()
#         else:
#             hora_temporal= datos['hora_muestreo'][idato]
        
#         # Por incompatibilidad con POSTGRESQL también hay que "desmontar" y volver a montar las horas
#         hora          = hora_temporal.hour
#         minuto        = hora_temporal.minute
#         hora_consulta = datetime.time(hora,minuto) 
            
#         instruccion_sql = "INSERT INTO muestreos_discretos (nombre_muestreo,estacion,fecha_muestreo,hora_muestreo,profundidad,botella,configuracion_perfilador,configuracion_superficie) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_muestreo,profundidad,configuracion_perfilador,configuracion_superficie) DO NOTHING;"   
#         cursor.execute(instruccion_sql, (nombre_muestreo,int(datos['id_estacion_temp'][idato]),fecha_consulta,hora_consulta,round(datos['profundidad'][idato],2),id_botella,int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
#         conn.commit()
 
#         instruccion_sql = "SELECT id_muestreo FROM muestreos_discretos WHERE estacion = %s AND fecha_muestreo = %s AND hora_muestreo=%s AND profundidad = %s AND configuracion_perfilador = %s AND configuracion_superficie = %s;"
#         cursor.execute(instruccion_sql, (int(datos['id_estacion_temp'][idato]),fecha_consulta,datos['hora_muestreo'][idato],round(datos['profundidad'][idato],2),int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
#         id_muestreos_bd =cursor.fetchone()
#         conn.commit()     
    

#     else:
        
#         instruccion_sql = "INSERT INTO muestreos_discretos (nombre_muestreo,estacion,fecha_muestreo,profundidad,botella,configuracion_perfilador,configuracion_superficie) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_muestreo,profundidad,configuracion_perfilador,configuracion_superficie) DO NOTHING;"   
#         cursor.execute(instruccion_sql, (nombre_muestreo,int(datos['id_estacion_temp'][idato]),fecha_consulta,round(datos['profundidad'][idato],2),id_botella,int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
#         conn.commit()
            
#         instruccion_sql = "SELECT id_muestreo FROM muestreos_discretos WHERE estacion = %s AND fecha_muestreo = %s AND profundidad = %s AND configuracion_perfilador = %s AND configuracion_superficie = %s;"
#         cursor.execute(instruccion_sql, (int(datos['id_estacion_temp'][idato]),fecha_consulta,round(datos['profundidad'][idato],2),int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
#         id_muestreos_bd =cursor.fetchone()
#         conn.commit() 
    
#     datos['id_muestreo_temp'][idato] =  id_muestreos_bd[0]

# cursor.close()
# conn.close() 

















# datos = evalua_registros(datos,direccion_host,base_datos,usuario,contrasena,puerto)



# import pandas.io.sql as psql
# from sqlalchemy import create_engine

# texto ='init' + (datetime.datetime.now()).strftime('%H:%M:%S')
  
# print('inicia', datetime.datetime.now())

# datos = evalua_estaciones(datos,min_dist,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)
 
# print('estaciones', datetime.datetime.now())

# datos = evalua_registros(datos,direccion_host,base_datos,usuario,contrasena,puerto)

# print('registros', datetime.datetime.now())

# print(datetime.datetime.now())

# # recupera las estaciones disponibles en la base de datos
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
# conn.close() 

# # Recorta el dataframe para tener sólo las estaciones del programa seleccionado
# estaciones_programa            = tabla_estaciones[tabla_estaciones['programa'] == id_programa]
# indices_dataframe              = numpy.arange(0,estaciones_programa.shape[0],1,dtype=int)    
# estaciones_programa['id_temp'] = indices_dataframe
# estaciones_programa.set_index('id_temp',drop=True,append=False,inplace=True)
    

# ## Identifica la estación asociada a cada registro

# # Columna para punteros de estaciones
# datos['id_estacion_temp'] = numpy.zeros(datos.shape[0],dtype=int) 

# # Genera un dataframe con la combinación única de latitud/longitud en los muestreos
# coordenadas_unicas            = datos.groupby(['latitud','longitud']).size().reset_index().rename(columns={0:'count'})
# coordenadas_unicas['identif'] = numpy.arange(0,coordenadas_unicas.shape[0],dtype=int)

# # Redondea a la misma precisión que las coordenadas almacenadas en la base de datos
# coordenadas_unicas = coordenadas_unicas.round({'longitud': 4, 'latitud': 4})

# # Busca los registros con la misma lat/lon que cada par de coordenadas únicas, a los que asigna un identificado (nº de estación)
# for idato in range(coordenadas_unicas.shape[0]):
#     aux = (datos['latitud'] == coordenadas_unicas['latitud'][idato]) & (datos['longitud'] == coordenadas_unicas['longitud'][idato])
#     indices = [i for i, x in enumerate(aux) if x]
#     datos['id_estacion_temp'][indices] = coordenadas_unicas['identif'][idato]

# print(datetime.datetime.now())



# proy_datos                = Proj(proj='utm',zone=29,ellps='WGS84', preserve_units=False) # Referencia coords

# for iregistro in range(datos.shape[0]): 
#     if datos_estaciones.shape[0] == 0:
                 
#         nueva_estacion = {'id_estacion':1,'nombre_estacion':datos['estacion'][iregistro], 'latitud':datos['latitud'][iregistro], 'longitud':datos['longitud'][iregistro], 'programa':id_programa}
# #        datos_estaciones = datos_estaciones.concat(nueva_estacion, ignore_index=True)
#         datos_estaciones.loc[1] = nueva_estacion       

#     else:
#         vector_distancias      = numpy.zeros(datos_estaciones.shape[0])
#         vector_identificadores = numpy.zeros(datos_estaciones.shape[0],dtype=int)
       
#         # Determina la distancia de cada registro a las estaciones incluidas en la base de datos
#         for iestacion_disponible in range(datos_estaciones.shape[0]):
#             x_muestreo, y_muestreo = proy_datos(datos['longitud'][iregistro], datos['latitud'][iregistro], inverse=False)
#             x_bd, y_bd             = proy_datos(datos_estaciones['longitud'][iestacion_disponible], datos_estaciones['latitud'][iestacion_disponible], inverse=False)
#             distancia              = math.sqrt((((x_muestreo-x_bd)**2) + ((y_muestreo-y_bd)**2)))
           
#             vector_distancias[iestacion_disponible]      = distancia
#             vector_identificadores[iestacion_disponible] = int(datos_estaciones['id_estacion'][iestacion_disponible])
           
#         # Si la distancia a alguna de las estaciones es menor a la distancia mínima, la estación ya está en la base de datos
#         if min(vector_distancias) <= min_dist :
#             ipos_minimo = numpy.argmin(vector_distancias)
#             datos['id_estacion_temp'][iregistro] = vector_identificadores[ipos_minimo]
           
#         # En caso contrario, la estación es nueva y se añade a la base de datos
#         else:
#             indice_insercion = int(max(datos_estaciones['id_estacion']) + 1)
 
#             nueva_estacion = {'id_estacion':indice_insercion,'nombre_estacion':datos['estacion'][iregistro], 'latitud':datos['latitud'][iregistro], 'longitud':datos['longitud'][iregistro], 'programa':id_programa}
#             #datos_estaciones = datos_estaciones.concat(nueva_estacion, ignore_index=True)
#             datos_estaciones.loc[datos_estaciones.shape[0]] = nueva_estacion 
         
#             datos['id_estacion_temp'][iregistro] = indice_insercion 


# # Inserta las nuevas estaciones
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# for iestacion in range(datos_estaciones.shape[0]):
#     datos_insercion = (int(datos_estaciones['id_estacion'][iestacion]),str(datos_estaciones['nombre_estacion'][iestacion]),round(datos_estaciones['latitud'][iestacion],4),round(datos_estaciones['longitud'][iestacion],4),int(id_programa))
#     instruccion_sql = "INSERT INTO estaciones (id_estacion,nombre_estacion,latitud,longitud,programa) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (id_estacion) DO NOTHING;"   
#     cursor.execute(instruccion_sql, (datos_insercion))
#     conn.commit() 
# cursor.close()
# conn.close()







