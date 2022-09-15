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
                                                    "SILCAT":"sio2","SILCAT_FLAG_W":"sio2_qf","NITRAT":"no3","NITRAT_FLAG_W":"no3_qf","NITRIT":"no2","NITRIT_FLAG_W":"no2_qf",
                                                    "PHSPHT":"po4","PHSPHT_FLAG_W":"po4_qf","TCARBN":"tcarbn","TCARBN_FLAG_W":"tcarbn_qf","ALKALI":"alkali","ALKALI_FLAG_W":"alkali_qf",
                                                    "PHTS25P0_UNPUR":"phts25p0_unpur","PHTS25P0_UNPUR_FLAG_W":"phts25p0_unpur_qf","PHTS25P0_PUR":"phts25p0_pur","PHTS25P0_PUR_FLAG_W":"phts25p0_pur_qf",
                                                    "R_CLOR":"r_clor","R_CLOR_FLAG_W":"r_clor_qf","R_PER":"r_per","R_PER_FLAG_W":"r_per_qf","CO3_TMP":"co3_temp"
                                                    })    
    
    
    # Añade una columan con el QF de la temperatura, igual al de la salinidad
    datos_radiales['temperatura_ctd_qf'] = datos_radiales['salinidad_ctd_qf'] 
    
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
                                                  "Latitud":"latitud","Longitud":"longitud","t_CTD":"temperatura_ctd","Sal_CTD":"salinidad_ctd","SiO2":"sio2","SiO2_flag":"sio2_qf",
                                                  "NO3":"no3","NO3T_flag":"no3_qf","NO2":"no2","NO2_flag":"no2_qf","NH4":"nh4","NH4_flag":"nh4_qf","PO4":"po4","PO4_flag":"po4_qf","Cla":"clorofila_a"
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
    datos_radprof['no3']    =  datos_radprof['NO3+NO2 umol/Kg'] - datos_radprof['NO2 umol/kg']
    datos_radprof['no3_qf'] =  datos_radprof['Flag_TON'] 
       
    # Renombra las columnas para mantener una denominación homogénea
    datos_radprof = datos_radprof.rename(columns={"ID":"nombre_muestreo","st":"estacion","Niskin":"botella",
                                                  "Lat":"latitud","Lon":"longitud","CTDPRS":"profundidad","CTDtemp":"temperatura_ctd","SALCTD":"salinidad_ctd",
                                                  "SiO2 umol/Kg":"sio2","Flag_SiO2":"sio2_qf",
                                                  "NO2 umol/kg":"no2","Flag_NO2":"no2_qf","PO4 umol/Kg":"po4","Flag_PO4":"po4_qf"
                                                  })
    
    
    # Mantén solo las columnas que interesan
    datos_radprof_recorte = datos_radprof[['nombre_muestreo', 'estacion','botella','fecha_muestreo','hora_muestreo','latitud','longitud','profundidad','temperatura_ctd','salinidad_ctd',
                                           'no3','no3_qf','no2','no2_qf','sio2','sio2_qf','po4','po4_qf','configuracion_superficie','configuracion_perfilador']]

    del(datos_radprof)

    return datos_radprof_recorte


##########################################################################
######## FUNCION PARA APLICAR CONTROL DE CALIDA BÁSICO LOS DATOS  ########
##########################################################################
def control_calidad(datos,direccion_host,base_datos,usuario,contrasena,puerto):
 
    textos_aviso = [] 
    
    # Recupera la tabla con los registros de muestreos físicos
    con_engine         = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql          = create_engine(con_engine)
    tabla_variables    = psql.read_sql('SELECT * FROM variables_procesado', conn_psql)    
        
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
    
    # Elimina registros duplicados en el mismo punto y a la misma hora(por precaucion)
    num_reg_inicial = datos.shape[0]
    datos           = datos.drop_duplicates(subset=['latitud','longitud','profundidad','fecha_muestreo','hora_muestreo'], keep='last')    
    num_reg_final   = datos.shape[0]
    if num_reg_final < num_reg_inicial:
        textos_aviso.append('Se han eliminado registros correspondientes a una misma fecha, hora,profundidad y estación')
    
    
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
        datos['profundidad'][idato] = round(datos['profundidad'][idato],2)      

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
    
    return datos  




###########################################################################
######## FUNCION PARA ENCONTRAR EL IDENTIFICADOR DE CADA REGISTRO  ########
###########################################################################

def evalua_registros(datos,nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto):
    
    # Recupera la tabla con los registros de los muestreos
    con_engine      = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql       = create_engine(con_engine)
    tabla_muestreos = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
    
    datos['id_muestreo_temp']  = numpy.zeros(datos.shape[0],dtype=int)
    
    # si no hay ningun valor en la tabla de registro, meter directamente todos los datos registrados
    if tabla_muestreos.shape[0] == 0:
    
        # genera un dataframe con las variables que interesa introducir en la base de datos
        exporta_registros                    = datos[['id_estacion_temp','fecha_muestreo','hora_muestreo','profundidad','botella','configuracion_perfilador','configuracion_superficie']]
        # añade el indice de cada registro
        indices_registros                    = numpy.arange(1,(exporta_registros.shape[0]+1))    
        exporta_registros['id_muestreo']     = indices_registros
        # renombra la columna con información de la estación muestreada
        exporta_registros                    = exporta_registros.rename(columns={"id_estacion_temp":"estacion"})
        # añade el nombre del muestreo
        exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
        for idato in range(exporta_registros.shape[0]):    
            exporta_registros['nombre_muestreo'][idato]  = nombre_programa + '_' + str(datos['fecha_muestreo'][idato].year) + '_E' + str(datos['id_estacion_temp'][idato])
            datos['id_muestreo_temp'] [idato]            = idato + 1
            
        # Inserta en base de datos        
        exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
        exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append') 
    
    # En caso contrario hay que ver registro a registro, si ya está incluido en la base de datos
    else:
    
        ultimo_registro_bd         = max(tabla_muestreos['id_muestreo'])
        datos['io_nuevo_muestreo'] = numpy.zeros(datos.shape[0],dtype=int)
            
        for idato in range(datos.shape[0]):
            df_temporal = tabla_muestreos.loc[(tabla_muestreos['estacion'] == datos['id_estacion_temp'][idato]) & (tabla_muestreos['fecha_muestreo'] == datos['fecha_muestreo'][idato]) & (tabla_muestreos['profundidad'] == datos['profundidad'][idato]) & (tabla_muestreos['configuracion_perfilador'] == datos['configuracion_perfilador'][idato])  & (tabla_muestreos['configuracion_superficie'] == datos['configuracion_superficie'][idato])]
            # Registro ya incluido, recuperar el identificador
            if df_temporal.shape[0] >0:
                datos['id_muestreo_temp'] [idato] =  df_temporal.iloc[0]['id_muestreo']
            # Nuevo registro
            else:
                # Asigna el identificador (siguiente al máximo disponible)
                ultimo_registro_bd                = ultimo_registro_bd + 1
                datos['id_muestreo_temp'][idato]  = ultimo_registro_bd               
                datos['io_nuevo_muestreo'][idato] = 1 
                
        
        if numpy.count_nonzero(datos['io_nuevo_muestreo']) > 0:
        
            # Genera un dataframe sólo con los valores nuevos, a incluir (io_nuevo_muestreo = 1)
            nuevos_muestreos  = datos[datos['io_nuevo_muestreo']==1]
            # Mantén sólo las columnas que interesan
            exporta_registros = nuevos_muestreos[['id_muestreo_temp','id_estacion_temp','fecha_muestreo','hora_muestreo','profundidad','botella','configuracion_perfilador','configuracion_superficie']]
            # Cambia el nombre de la columna de estaciones
            exporta_registros = exporta_registros.rename(columns={"id_estacion_temp":"estacion","id_muestreo_temp":"id_muestreo"})
            # Indice temporal
            exporta_registros['indice_temporal'] = numpy.arange(0,exporta_registros.shape[0])
            exporta_registros.set_index('indice_temporal',drop=True,append=False,inplace=True)
            # añade el nombre del muestreo
            exporta_registros['nombre_muestreo'] = [None]*nuevos_muestreos.shape[0]
            for idato in range(exporta_registros.shape[0]):    
                exporta_registros['nombre_muestreo'][idato]         = nombre_programa + '_' + str(exporta_registros['fecha_muestreo'][idato].year) + '_E' + str(exporta_registros['estacion'][idato])
    
    
            # # Inserta el dataframe resultante en la base de datos 
            exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
            exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append')    


    return datos



################################################################################
######## FUNCION PARA INSERTAR LOS DATOS DE FISICA EN LA BASE DE DATOS  ########
################################################################################

def inserta_datos_fisica(datos,direccion_host,base_datos,usuario,contrasena,puerto):
  
    # Recupera la tabla con los registros de muestreos físicos
    con_engine                = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql                      = create_engine(con_engine)
    tabla_registros_fisica    = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn_psql)
    
    # Genera un dataframe solo con las variales fisicas de los datos a importar 
    datos_fisica = datos[['temperatura_ctd', 'temperatura_ctd_qf','salinidad_ctd','salinidad_ctd_qf','par_ctd','par_ctd_qf','turbidez_ctd','turbidez_ctd_qf','id_muestreo_temp']]
    datos_fisica = datos_fisica.rename(columns={"id_muestreo_temp": "muestreo"})
    
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
                                 'no3','no3_qf','no2','no2_qf','nh4','nh4_qf','po4','po4_qf','sio2','sio2_qf','tcarbn','tcarbn_qf','doc','doc_qf',
                                 'cdom','cdom_qf','clorofila_a','clorofila_a_qf','alkali','alkali_qf','phts25p0_unpur','phts25p0_unpur_qf','phts25p0_pur','phts25p0_pur_qf','r_clor','r_clor_qf','r_per','r_per_qf','co3_temp']]    
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
            datos_bd = [None]*8
                
            # Genera el vector con los datos a insertar. diferente según sea análisis o post-procesado
            if itipo_informacion == 1: # La información a insertar es un nuevo registro de análisis de laboratorio
                datos_insercion     = [int(id_programa),nombre_programa,int(anho_procesado),fecha_final_muestreo,fecha_actualizacion,None,email_contacto,None]
            
            if itipo_informacion == 2: # La información a insertar es un registro de post-procesado 
                datos_insercion     = [int(id_programa),nombre_programa,int(anho_procesado),fecha_final_muestreo,None,fecha_actualizacion,None,email_contacto]
            
            # Inserta la información en la base de datos
            instruccion_sql = "INSERT INTO estado_procesos (programa,nombre_programa,año,fecha_final_muestreo,fecha_analisis_laboratorio,fecha_post_procesado,contacto_muestreo,contacto_post_procesado) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,año) DO UPDATE SET (nombre_programa,fecha_final_muestreo,fecha_analisis_laboratorio,fecha_post_procesado,contacto_muestreo,contacto_post_procesado) = (EXCLUDED.nombre_programa,EXCLUDED.fecha_final_muestreo,EXCLUDED.fecha_analisis_laboratorio,EXCLUDED.fecha_post_procesado,EXCLUDED.contacto_muestreo,EXCLUDED.contacto_post_procesado);"   
            cursor.execute(instruccion_sql, (datos_insercion))
            conn.commit()

        # Si la base de datos ya contiene registros del programa y año a insertar, actualizar las fechas correspondientes
        else:
            if itipo_informacion == 1:
                instruccion_sql = "UPDATE estado_procesos SET fecha_analisis_laboratorio = %s,contacto_muestreo = %s WHERE programa = %s AND año = %s;"   
                cursor.execute(instruccion_sql, (fecha_actualizacion,email_contacto,int(id_programa),int(anho_procesado)))
                conn.commit()                

            if itipo_informacion == 2:
                instruccion_sql = "UPDATE estado_procesos SET fecha_post_procesado = %s,contacto_post_procesado = %s WHERE programa = %s AND año = %s;"   
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
 
# # nombre_archivo = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/RADIAL_BTL_COR_2013.xlsx'   
# # datos          = lectura_datos_radiales(nombre_archivo,direccion_host,base_datos,usuario,contrasena,puerto) 
# # datos,textos_aviso = control_calidad(datos_radiales,direccion_host,base_datos,usuario,contrasena,puerto)
# # id_programa = 3
# # nombre_programa = "RADIAL CORUÑA"

# nombre_archivo = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx'   
# datos_pelacus = lectura_datos_pelacus(nombre_archivo)    
# datos,textos_aviso = control_calidad(datos_pelacus,direccion_host,base_datos,usuario,contrasena,puerto)
# id_programa = 1
# nombre_programa = "PELACUS"

# # nombre_archivo = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADPROF/RADPROF_2021.xlsm'   
# # datos          = lectura_datos_radprof(nombre_archivo) 
# # datos,textos_aviso = control_calidad(datos,direccion_host,base_datos,usuario,contrasena,puerto)
# # id_programa = 5
# # nombre_programa = "RADPROF"






# # # # print('inicio',datetime.datetime.now())

# datos = evalua_estaciones(datos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)  

# # print('evalua estaciones',datetime.datetime.now())

# # datos = evalua_registros(datos,nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# # # print('evalua registros',datetime.datetime.now())






