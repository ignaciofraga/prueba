# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 08:08:39 2022

@author: ifraga
"""

import numpy
import pandas
import datetime
import math
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine
import json
import seawater
import streamlit
from io import BytesIO

from matplotlib.ticker import FormatStrFormatter

pandas.options.mode.chained_assignment = None

#######################################################################################
######## FUNCION PARA APLICAR CONTROL DE CALIDAD BÁSICO LOS DATOS DE PROGRAMAS  #######
#######################################################################################
def control_calidad(datos,direccion_host,base_datos,usuario,contrasena,puerto):
 
    textos_aviso = [] 
        
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


    # Cambia todos los -999 por None
    datos = datos.replace(-999, None) 
    
    # # Añade las columnas con datos de qf=9 en aquellas variables que no estén incluidas
    # con_engine         = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    # conn_psql          = create_engine(con_engine)
    # tabla_variables    = psql.read_sql('SELECT * FROM variables_procesado', conn_psql)    
    # conn_psql.dispose()
        
    # listado_variables_fisicas = tabla_variables['variables_fisicas']
    # for variable in listado_variables_fisicas:
    #     if variable is not None and variable.endswith("_qf") and variable not in datos.columns.tolist(): 
    #         datos[variable] = 9 

    # listado_variables_bgq = tabla_variables['variables_biogeoquimicas']
    # for variable in listado_variables_bgq:
    #     if variable is not None and variable.endswith("_qf") and variable not in datos.columns.tolist(): 
    #         datos[variable] = 9     

    return datos,textos_aviso    
 
    
 






 
 






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







 
  
##############################################################################
######## FUNCION PARA ENCONTRAR LA ESTACIÓN ASOCIADA A CADA REGISTRO  ########
##############################################################################

def evalua_estaciones(datos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto):

    # Consulta las estaciones disponibles en la base de datos    
    con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql        = create_engine(con_engine)
    tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql) 
        
    # Cambia nombres a minusculas para comparar 
    tabla_estaciones['nombre_estacion'] = tabla_estaciones['nombre_estacion'].apply(lambda x:x.lower())
    
    # Cambia los nombres a minusculas
    datos['estacion'] = datos['estacion'].astype(str)
    datos['estacion'] = datos['estacion'].apply(lambda x:x.lower())
    
    # Columna para punteros de estaciones
    datos['id_estacion_temp'] = numpy.zeros(datos.shape[0],dtype=int) 
    
    # Recorta el dataframe para tener sólo las estaciones del programa seleccionado
    estaciones_programa            = tabla_estaciones[tabla_estaciones['programa'] == id_programa]
    indices_dataframe              = numpy.arange(0,estaciones_programa.shape[0],1,dtype=int)    
    estaciones_programa['id_temp'] = indices_dataframe
    estaciones_programa.set_index('id_temp',drop=True,append=False,inplace=True)
    
    io_lat = 0
    io_lon = 0
    listado_variables = datos.columns.tolist()
    if 'latitud' not in listado_variables:
        datos['latitud'] = [None]*datos.shape[0]
        io_lat           = 1
    if 'longitud' not in listado_variables:
        datos['longitud'] = [None]*datos.shape[0]
        io_lon            = 1    
        
    # Genera un dataframe con las estaciones incluidas en el muestreo
    estaciones_muestreadas                      = datos['estacion'].unique()
    estaciones_muestreadas                      = pandas.DataFrame(data=estaciones_muestreadas,columns=['estacion'])  
    estaciones_muestreadas['id_estacion']       = numpy.zeros(estaciones_muestreadas.shape[0],dtype=int)
    estaciones_muestreadas['io_nueva_estacion'] = numpy.zeros(estaciones_muestreadas.shape[0],dtype=int)
    estaciones_muestreadas['latitud_estacion']  = numpy.zeros(estaciones_muestreadas.shape[0],dtype=int)
    estaciones_muestreadas['longitud_estacion'] = numpy.zeros(estaciones_muestreadas.shape[0],dtype=int)
    estaciones_muestreadas = estaciones_muestreadas.rename(columns={"estacion":"nombre_estacion"})


    # Contadores e identificadores 
    if len(tabla_estaciones['id_estacion'])>0:
        id_ultima_estacion_bd = max(tabla_estaciones['id_estacion'])
    else:
        id_ultima_estacion_bd = 0
        
    iconta_nueva_estacion     = 1
    
    # Encuentra el identificador asociado a cada estacion en la base de datos
    for iestacion in range(estaciones_muestreadas.shape[0]):
        
        df_temporal = estaciones_programa[estaciones_programa['nombre_estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]]
    
        # Estacion ya incluida en la base de datos. Recuperar identificador
        if df_temporal.shape[0]>0:
            estaciones_muestreadas['id_estacion'][iestacion]       = df_temporal['id_estacion'].iloc[0]
            
            # Asigna lat/lon a la medida si ésta no la tenía
            if io_lat == 1:
                datos['latitud'][datos['estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]] =  df_temporal['latitud_estacion'].iloc[0] 
            if io_lon == 1:
                datos['longitud'][datos['estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]] =  df_temporal['longitud_estacion'].iloc[0] 
            
             
        # Nueva estación, asignar orden creciente de identificador
        else:
            estaciones_muestreadas['id_estacion'][iestacion]       = id_ultima_estacion_bd + iconta_nueva_estacion
            estaciones_muestreadas['io_nueva_estacion'][iestacion] = 1           
            iconta_nueva_estacion                                 = iconta_nueva_estacion + 1
  
            # Determina la lat/lon de la estacion a partir de los valores de los registros asociados
            estaciones_muestreadas['latitud_estacion'][iestacion]  = (datos['latitud'][datos['estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]]).mean()
            estaciones_muestreadas['longitud_estacion'][iestacion] = (datos['longitud'][datos['estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]]).mean()
          
        # Asigna el identificador de estación a los datos importados
        datos['id_estacion_temp'][datos['estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]]=estaciones_muestreadas['id_estacion'][iestacion]
            


    # Añade en la base de datos las nuevas estaciones    
    if numpy.count_nonzero(estaciones_muestreadas['io_nueva_estacion']) > 0:
    
        # Genera un dataframe sólo con los valores nuevos, a incluir (io_nuevo_muestreo = 1)
        nuevos_muestreos  = estaciones_muestreadas[estaciones_muestreadas['io_nueva_estacion']==1]
        # Mantén sólo las columnas que interesan
        exporta_registros = nuevos_muestreos[['id_estacion','nombre_estacion','latitud_estacion','longitud_estacion']]
        # Añade columna con el identiicador del programa
        exporta_registros['programa'] = numpy.zeros(exporta_registros.shape[0],dtype=int)
        exporta_registros['programa'] = id_programa
        # corrije el indice del dataframe 
        exporta_registros.set_index('id_estacion',drop=True,append=False,inplace=True)
    
        # Inserta el dataframe resultante en la base de datos 
        exporta_registros.to_sql('estaciones', conn_psql,if_exists='append')

    # elimina la informacion cargada y que no se vaya a exportar, para liberar memoria
    del(estaciones_muestreadas,estaciones_programa,tabla_estaciones)
    
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
    tabla_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn_psql)
    conn_psql.dispose()

    datos['id_salida']  = numpy.zeros(datos.shape[0],dtype=int)
    
    # Contadores e identificadores 
    if tabla_salidas['id_salida'].shape[0]>0:
        id_ultima_salida_bd = max(tabla_salidas['id_salida'])
    else:
        id_ultima_salida_bd = 0
    iconta_nueva_salida     = 1
    
    # listado con los nombres de meses
    meses = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']
           
    # Extrae el mes y año de cada salida al mar
    datos['mes'] = numpy.zeros(datos.shape[0])
    datos['año'] = numpy.zeros(datos.shape[0])
    for idato in range(datos.shape[0]):
        datos['mes'].iloc[idato] =  datos['fecha_muestreo'].iloc[idato].month    
        datos['año'].iloc[idato] =  datos['fecha_muestreo'].iloc[idato].year 
    
 
    if tipo_salida == 'ANUAL':

        anhos_salida_mar = datos['año'].unique()
        
        for ianho in range(len(anhos_salida_mar)):    
 
            subset_anual     = datos[datos['año']==anhos_salida_mar[ianho]] 
            
            # Busca las fechas de salida y llegada
            fechas_anuales   = subset_anual['fecha_muestreo'].unique()
            fecha_salida     = min(fechas_anuales)
            fecha_llegada    = max(fechas_anuales)            
            
            df_temporal = tabla_salidas[tabla_salidas['fecha_salida']==fecha_salida]
    
            # Salida ya incluida en la base de datos. Recuperar identificador
            if df_temporal.shape[0]>0:
                id_salida = df_temporal['id_salida'].iloc[0]
                
            # Salida no incluida. Añadirla a la base de datos.
            else:
            
                id_salida           = id_ultima_salida_bd + iconta_nueva_salida
                iconta_nueva_salida = iconta_nueva_salida + 1
            
                # Encuentra las estaciones muestreadas
                subset_salida                        = datos[datos['año']==anhos_salida_mar[ianho]]
                identificador_estaciones_muestreadas = list(subset_salida['id_estacion_temp'].unique())
                estaciones_muestreadas               =[None]*len(identificador_estaciones_muestreadas)
                for iestacion in range(len(estaciones_muestreadas)):
                    estaciones_muestreadas[iestacion] = tabla_estaciones['nombre_estacion'][tabla_estaciones['id_estacion']==identificador_estaciones_muestreadas[iestacion]].iloc[0]
                json_estaciones        = json.dumps(estaciones_muestreadas)
                   
                # Define nombre
                nombre_salida = nombre_programa + ' ' + str(anhos_salida_mar[ianho])
          
                # Inserta en la base de datos
                conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                cursor = conn.cursor()                      
                instruccion_sql = '''INSERT INTO salidas_muestreos (id_salida,nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO NOTHING;'''        
                cursor.execute(instruccion_sql, (int(id_salida),nombre_salida,int(id_programa),nombre_programa,tipo_salida,fecha_salida,fecha_llegada,json_estaciones))
                conn.commit()
                cursor.close()
                conn.close()
                
            # Asigna el id de la salida al dataframe
            datos['id_salida'][datos['año']==anhos_salida_mar[ianho]] = id_salida
    
            
    if tipo_salida == 'MENSUAL' : # Programa radiales Coruña

        anhos_salida_mar = datos['año'].unique()
        
        for ianho in range(len(anhos_salida_mar)):    
 
            subset_anual     = datos[datos['año']==anhos_salida_mar[ianho]] 
            
            if id_programa == 3: # PROGRAMA RADIALES CORUÑA. busco las salidas por fechas únicas (salidas de 1 día)
            
                fechas_salidas_mar = subset_anual['fecha_muestreo'].unique()
                fechas_partida     = fechas_salidas_mar
                fechas_regreso     = fechas_salidas_mar
            
            if id_programa == 4: # PROGRAMA RADIALES CANTABRICO. busco las salidas por meses únicos (salidas de más de 1 día, no vale criterio anterior)
            
                meses_salida_mar = subset_anual['mes'].unique()        
                fechas_partida   = []
                fechas_regreso   = []
                for imes in range(len(meses_salida_mar)):            
                    subset_mensual    = subset_anual[subset_anual['mes']==meses_salida_mar[imes]]
                    fechas_partida    = fechas_partida + [min(subset_mensual['fecha_muestreo'])]
                    fechas_regreso    = fechas_regreso + [max(subset_mensual['fecha_muestreo'])]       
        
            for isalida in range(len(fechas_partida)):            
    
                df_temporal = tabla_salidas[tabla_salidas['fecha_salida']==fechas_partida[isalida]]
        
                # Salida ya incluida en la base de datos. Recuperar identificador
                if df_temporal.shape[0]>0:
                    id_salida = df_temporal['id_salida'].iloc[0]
                    
                # Salida no incluida. Añadirla a la base de datos.
                else:
                
                    id_salida           = id_ultima_salida_bd + iconta_nueva_salida
                    iconta_nueva_salida = iconta_nueva_salida + 1
                
                    # Encuentra las estaciones muestreadas
                    subset_salida                        = datos[datos['fecha_muestreo']==fechas_salidas_mar[isalida]]
                    identificador_estaciones_muestreadas = list(subset_salida['id_estacion_temp'].unique())
                    estaciones_muestreadas               =[None]*len(identificador_estaciones_muestreadas)
                    for iestacion in range(len(estaciones_muestreadas)):
                        estaciones_muestreadas[iestacion] = tabla_estaciones['nombre_estacion'][tabla_estaciones['id_estacion']==identificador_estaciones_muestreadas[iestacion]].iloc[0]
                    json_estaciones        = json.dumps(estaciones_muestreadas)
                       
                    # Define nombre
                    nombre_salida = nombre_programa + ' ' + tipo_salida + ' ' +   str(meses[fechas_salidas_mar[isalida].month-1]) + ' ' +  str(fechas_salidas_mar[isalida].year)
              
                    # Inserta en la base de datos
                    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                    cursor = conn.cursor()                      
                    instruccion_sql = '''INSERT INTO salidas_muestreos (id_salida,nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO NOTHING;'''        
                    cursor.execute(instruccion_sql, (int(id_salida),nombre_salida,int(id_programa),nombre_programa,tipo_salida,fechas_salidas_mar[isalida],fechas_salidas_mar[isalida],json_estaciones))
                    conn.commit()
                    cursor.close()
                    conn.close()
                    
                # Asigna el id de la salida al dataframe
                if id_programa == 3:
                    datos['id_salida'][datos['fecha_muestreo']==fechas_salidas_mar[isalida]] = id_salida
                if id_programa == 4:                
                    datos['id_salida'][(datos['año']==anhos_salida_mar[ianho]) & (datos['mes']==fechas_partida[isalida].month)] = id_salida
      
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
       
    datos['muestreo']  = numpy.zeros(datos.shape[0],dtype=int)
    
    # si no hay ningun valor en la tabla de registro, meter directamente todos los datos registrados
    if tabla_muestreos.shape[0] == 0:
    
        # genera un dataframe con las variables que interesa introducir en la base de datos
        exporta_registros                    = datos[['id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','latitud','longitud']]
        # añade el indice de cada registro
        indices_registros                    = numpy.arange(1,(exporta_registros.shape[0]+1))    
        exporta_registros['muestreo']     = indices_registros
        # renombra la columna con información de la estación muestreada
        exporta_registros                    = exporta_registros.rename(columns={"id_estacion_temp":"estacion",'id_salida':'salida_mar','latitud':'latitud_muestreo','longitud':'longitud_muestreo'})
        # # añade el nombre del muestreo
        exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
        for idato in range(exporta_registros.shape[0]):    
            nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'].iloc[idato]]['nombre_estacion'].iloc[0]
            
            nombre_muestreo     = abreviatura_programa + '_' + datos['fecha_muestreo'].iloc[idato].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
            if datos['num_cast'].iloc[idato] is not None:
                nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'].iloc[idato]))
            else:
                nombre_muestreo = nombre_muestreo + '_C1' 
                
            if datos['botella'].iloc[idato] is not None:
                nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'].iloc[idato])) 
            else:
                if datos['prof_referencia'].iloc[idato] is not None: 
                    nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['prof_referencia'].iloc[idato]))
                else:
                    nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['presion_ctd'].iloc[idato])) 
                
            exporta_registros['nombre_muestreo'].iloc[idato]  = nombre_muestreo

            datos['muestreo'].iloc[idato]                 = idato + 1
            
            
        # Inserta en base de datos        
        exporta_registros.set_index('muestreo',drop=True,append=False,inplace=True)
        exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append') 
    
    # En caso contrario hay que ver registro a registro, si ya está incluido en la base de datos
    else:
    
        ultimo_registro_bd         = max(tabla_muestreos['muestreo'])
        datos['io_nuevo_muestreo'] = numpy.ones(datos.shape[0],dtype=int)

        for idato in range(datos.shape[0]):

            if datos['botella'].iloc[idato] is not None:        
                if datos['hora_muestreo'].iloc[idato] is not None:          
                    df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['botella']==datos['botella'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) & (tabla_muestreos['hora_muestreo']==datos['hora_muestreo'].iloc[idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]
    
                else:
                    df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['botella']==datos['botella'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]
            else:
                if datos['hora_muestreo'].iloc[idato] is not None:          
                    df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) & (tabla_muestreos['hora_muestreo']==datos['hora_muestreo'].iloc[idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]
    
                else:
                    df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]
               
            
            if df_temp.shape[0]> 0:
                datos['muestreo'].iloc[idato]          = df_temp['muestreo'].iloc[0]    
                datos['io_nuevo_muestreo'].iloc[idato] = 0
                
            else:
                datos['io_nuevo_muestreo'].iloc[idato] = 1
                ultimo_registro_bd                     = ultimo_registro_bd + 1
                datos['muestreo'].iloc[idato]       = ultimo_registro_bd 
            
        
        if numpy.count_nonzero(datos['io_nuevo_muestreo']) > 0:
        
            # Genera un dataframe sólo con los valores nuevos, a incluir (io_nuevo_muestreo = 1)
            nuevos_muestreos  = datos[datos['io_nuevo_muestreo']==1]
            # Mantén sólo las columnas que interesan
            exporta_registros = nuevos_muestreos[['muestreo','id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','latitud','longitud']]
                        
            # Cambia el nombre de la columna de estaciones
            exporta_registros = exporta_registros.rename(columns={"id_estacion_temp":"estacion",'id_salida':'salida_mar','latitud':'latitud_muestreo','longitud':'longitud_muestreo'})
            # Indice temporal
            exporta_registros['indice_temporal'] = numpy.arange(0,exporta_registros.shape[0])
            exporta_registros.set_index('indice_temporal',drop=True,append=False,inplace=True)
            # Añade el nombre del muestreo
            exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
            for idato in range(exporta_registros.shape[0]):    
                nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'].iloc[idato]]['nombre_estacion'].iloc[0]
              
                nombre_muestreo     = abreviatura_programa + '_' + datos['fecha_muestreo'].iloc[idato].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
                if datos['num_cast'].iloc[idato] is not None:
                    nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'].iloc[idato]))
                else:
                    nombre_muestreo = nombre_muestreo + '_C1'     
                
                if datos['botella'].iloc[idato] is not None:
                    nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'].iloc[idato])) 
                else:
                    if datos['prof_referencia'].iloc[idato] is not None: 
                        nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['prof_referencia'].iloc[idato]))
                    else:
                        nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['presion_ctd'].iloc[idato])) 
                 
                exporta_registros['nombre_muestreo'].iloc[idato]  = nombre_muestreo
        
            # # Inserta el dataframe resultante en la base de datos 
            exporta_registros.set_index('muestreo',drop=True,append=False,inplace=True)
            exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append')    
    
    conn_psql.dispose() # Cierra la conexión con la base de datos  
    
    return datos


############################################################################
######## FUNCION PARA INSERTAR DATOS DISCRETOS EN LA BASE DE DATOS  ########
############################################################################

def inserta_datos(datos_insercion,tipo_datos,direccion_host,base_datos,usuario,contrasena,puerto):
  
    if tipo_datos     == 'discreto_fisica':
        variables     = 'variables_fisicas'
        tabla_destino = 'datos_discretos_fisica'
        puntero       = 'muestreo'
    
    elif tipo_datos   == 'discreto_bgq':
        variables     = 'variables_biogeoquimicas'  
        tabla_destino = 'datos_discretos_biogeoquimica'
        puntero       = 'muestreo'
        
    elif tipo_datos   == 'perfil_fisica':
        variables     = 'variables_fisicas'  
        tabla_destino = 'datos_perfil_fisica'
        puntero       = 'perfil'            
            
    elif tipo_datos   == 'perfil_bgq':
        variables     = 'variables_biogeoquimicas'  
        tabla_destino = 'datos_perfil_biogeoquimica'
        puntero       = 'perfil'
        
    # Recupera la tabla con los registros de muestreos físicos
    con_engine         = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql          = create_engine(con_engine)
    tabla_variables    = psql.read_sql('SELECT * FROM variables_procesado', conn_psql) 
    instruccion_sql    = 'SELECT * FROM ' + tabla_destino
    tabla_registros    = psql.read_sql(instruccion_sql, conn_psql)
    conn_psql.dispose()
        
    # Lee las variables de cada tipo a utilizar en el control de calidad
    variables_bd  = [x for x in tabla_variables[variables] if str(x) != 'None']    
  
    # Busca qué variables están incluidas en los datos a importar
    listado_variables_datos   = datos_insercion.columns.tolist()
    listado_variables_comunes = list(set(listado_variables_datos).intersection(variables_bd))
    listado_adicional         = [puntero] + listado_variables_comunes
    
    # # Si no existe ningún registro en la base de datos, introducir todos los datos disponibles
    if tabla_registros.shape[0] == 0:
        
        datos_insercion = datos_insercion[listado_adicional]
        
        datos_insercion.set_index(puntero,drop=True,append=False,inplace=True)
        datos_insercion.to_sql(tabla_destino, conn_psql,if_exists='append')
                        
    # En caso contrario, comprobar qué parte de la información está en la base de datos
    else: 
        
        for idato in range(datos_insercion.shape[0]): # Dataframe con la interseccion de los datos nuevos y los disponibles en la base de datos, a partir de la variable muestreo
         
            df_temp  = tabla_registros[(tabla_registros[puntero]==datos_insercion[puntero].iloc[idato])] 
            
            if df_temp.shape[0]>0:  # Muestreo ya incluido en la base de datos
            
                muestreo = df_temp[puntero].iloc[0]
                
                for ivariable in range(len(listado_variables_comunes)): # Reemplazar las variables disponibles en el muestreo correspondiente
                        
                    #tabla_registros[listado_variables_comunes[ivariable]][tabla_registros[puntero]==int(muestreo)] = datos_insercion[listado_variables_comunes[ivariable]][datos_insercion[puntero]==int(muestreo)]
                    tabla_registros[listado_variables_comunes[ivariable]][tabla_registros[puntero]==int(muestreo)] = datos_insercion[listado_variables_comunes[ivariable]].iloc[idato]

            
            else: # Nuevo muestreo
                       
                df_add = datos_insercion[datos_insercion[puntero]==datos_insercion[puntero].iloc[idato]] # Genero un dataframe con cada línea de datos a añadir
  
                df_add = df_add[listado_adicional] # Recorto para que tenga sólo las variables a añadir
            
                tabla_registros = pandas.concat([tabla_registros, df_add]) # Combino ambos dataframes
                   
        tabla_registros.set_index(puntero,drop=True,append=False,inplace=True)
        
        # borra los registros existentes en la tabla (no la tabla en sí, para no perder tipos de datos y referencias)
        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
        cursor = conn.cursor()
        instruccion_sql = "TRUNCATE " + tabla_destino + ";"
        cursor.execute(instruccion_sql)
        conn.commit()
        cursor.close()
        conn.close() 
        
        # Inserta el dataframe resultante en la base de datos 
        tabla_registros.to_sql(tabla_destino, conn_psql,if_exists='append')
  

    conn_psql.dispose() # Cierra la conexión con la base de datos 









###############################################################################
###### FUNCION PARA REALIZAR CONTROL DE CALDIAD DE DATOS BIOGEOQUIMICOS #######
###############################################################################
def control_calidad_biogeoquimica(datos_procesados,datos_disponibles_bd,variable_procesada,nombre_completo_variable_procesada,unidades_variable,df_indices_calidad,meses_offset,tabla_insercion):

    import streamlit as st
    import matplotlib.pyplot as plt
    
    def rango_datos(datos_procesados,datos_disponibles_bd,variable_procesada,df_indices_calidad,io_malos,io_dudosos,io_no_eval):

        fmin                      = 0.9
        fmax                      = 1.1       

        id_dato_malo              = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Malo'].iloc[0]
        id_dato_bueno             = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Bueno'].iloc[0]
        id_dato_dudoso            = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Dudoso'].iloc[0]
        id_dato_no_eval           = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='No evaluado'].iloc[0]

        # Selecciona el rango del gráfico
        min_seleccion = numpy.nanmin(numpy.array(datos_procesados[variable_procesada]))
        max_seleccion = numpy.nanmax(numpy.array(datos_procesados[variable_procesada]))
        df_datos_buenos = datos_disponibles_bd[datos_disponibles_bd[qf_variable_procesada]==id_dato_bueno]
        if df_datos_buenos.shape[0] > 0:
            min_bd    = numpy.nanmin(numpy.array(df_datos_buenos[variable_procesada]))
            max_bd    = numpy.nanmax(numpy.array(df_datos_buenos[variable_procesada]))
            min_val   = fmin*min(min_bd,min_seleccion)
            max_val   = fmax*max(max_bd,max_seleccion)
        else:
            min_val    = fmin*min_seleccion 
            max_val    = fmax*min_seleccion 
           
        if io_malos:
            df_datos_malos = datos_disponibles_bd[datos_disponibles_bd[variable_procesada]==id_dato_malo]
            if df_datos_malos.shape[0] > 0:
                min_bd         = numpy.nanmin(numpy.array(df_datos_malos[variable_procesada]))
                max_bd         = numpy.nanmax(numpy.array(df_datos_malos[variable_procesada]))
                min_val        = fmin*min(min_val,min_seleccion)
                max_val        = fmax*max(max_val,max_seleccion)  
    
        if io_dudosos:
            df_datos_dudosos = datos_disponibles_bd[datos_disponibles_bd[variable_procesada]==id_dato_dudoso]
            if df_datos_dudosos.shape[0] > 0:
                min_bd           = numpy.nanmin(numpy.array(df_datos_dudosos[variable_procesada]))
                max_bd           = numpy.nanmax(numpy.array(df_datos_dudosos[variable_procesada]))
                min_val          = fmin*min(min_val,min_seleccion)
                max_val          = fmax*max(max_val,max_seleccion) 
            
        if io_no_eval:
            df_datos_no_eval = datos_disponibles_bd[datos_disponibles_bd[variable_procesada]==id_dato_no_eval]
            if df_datos_no_eval.shape[0] > 0:
                min_bd           = numpy.nanmin(numpy.array(df_datos_no_eval[variable_procesada]))
                max_bd           = numpy.nanmax(numpy.array(df_datos_no_eval[variable_procesada]))
                min_val          = fmin*min(min_val,min_seleccion)
                max_val          = fmax*max(max_val,max_seleccion)

        return min_val,max_val
    
    

    

    id_dato_malo              = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Malo'].iloc[0]
    id_dato_bueno             = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Bueno'].iloc[0]
    id_dato_dudoso            = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Dudoso'].iloc[0]
    id_dato_no_eval           = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='No evaluado'].iloc[0]

    qf_variable_procesada     = variable_procesada + '_qf'

    df_datos_no_eval = datos_disponibles_bd[datos_disponibles_bd[variable_procesada]==id_dato_no_eval]
    df_datos_malos = datos_disponibles_bd[datos_disponibles_bd[variable_procesada]==id_dato_malo]
    df_datos_dudosos = datos_disponibles_bd[datos_disponibles_bd[variable_procesada]==id_dato_dudoso]
    df_datos_buenos = datos_disponibles_bd[datos_disponibles_bd[qf_variable_procesada]==id_dato_bueno]
    

    # comprueba si hay datos de la variable a analizar en la salida seleccionada
    if datos_disponibles_bd[variable_procesada].isnull().all():
        texto_error = "La base de datos no contiene información para la variable, salida y estación seleccionadas"
        st.warning(texto_error, icon="⚠️")

    else:

        # Determina los meses que marcan el rango de busqueda
        datos_procesados = datos_procesados.sort_values('fecha_muestreo')
        fecha_minima     = datos_procesados['fecha_muestreo'].iloc[0] - datetime.timedelta(days=meses_offset*30)
        fecha_maxima     = datos_procesados['fecha_muestreo'].iloc[-1] + datetime.timedelta(days=meses_offset*30)  
        
        if fecha_minima.year < fecha_maxima.year:
            listado_meses_1 = numpy.arange(fecha_minima.month,13)
            listado_meses_2 = numpy.arange(1,fecha_maxima.month+1)
            listado_meses   = numpy.concatenate((listado_meses_1,listado_meses_2))
        
        else:
            listado_meses   = numpy.arange(fecha_minima.month,fecha_maxima.month+1)
     
        listado_meses = listado_meses.tolist()
       
        # Genera un dataframe sólo con los datos "buenos"        
        df_datos_buenos = datos_disponibles_bd[datos_disponibles_bd[qf_variable_procesada]==id_dato_bueno]
                
        # Busca los datos de la base de datos dentro del rango de meses seleccionados
        df_datos_buenos['mes']  = pandas.DatetimeIndex(df_datos_buenos['fecha_muestreo']).month
        df_rango_temporal       = df_datos_buenos[df_datos_buenos['mes'].isin(listado_meses)]
                   
        # Líneas para separar un poco la parte gráfica de la de entrada de datos        
        for isepara in range(4):
            st.text(' ')
               
        with st.expander("Ajustar estilo de gráficos",expanded=False):
        
            st.write("Selecciona los datos a mostrar según su bandera de calidad")    
        
            # Selecciona mostrar o no datos malos y dudosos
            col1, col2, col3, col4, col5, col6 = st.columns(6,gap="small")
            with col1:
                io_buenos   = st.checkbox('Buenos', value=True)
                io_malos    = st.checkbox('Malos', value=False) 
            with col2:
                color_buenos = st.color_picker('Color', '#C0C0C0',label_visibility="collapsed")
                color_malos  = st.color_picker('Color', '#00CCCC',label_visibility="collapsed")
            with col3:
                io_rango      = st.checkbox('Buenos(intervalo)', value=True)
                io_dudosos    = st.checkbox('Dudosos', value=True)
            with col4:
                color_rango   = st.color_picker('Color', '#404040',label_visibility="collapsed")
                color_dudosos = st.color_picker('Color', '#00f900',label_visibility="collapsed")
            with col5:
                io_no_eval    = st.checkbox('No evaluados', value=True)
            with col6:
                color_no_eval = st.color_picker('Color', '#03b6fc',label_visibility="collapsed")

            
        texto_rango = 'Ajustar rango del gráfico ' + variable_procesada.upper() + ' vs PROFUNDIDAD'
        with st.expander(texto_rango,expanded=False):            
            
            st.write("Selecciona el rango del gráfico") 
            
            min_val,max_val = rango_datos(datos_procesados,datos_disponibles_bd,variable_procesada,df_indices_calidad,io_malos,io_dudosos,io_no_eval)
           
                               
            rango   = (max_val-min_val)
            min_val = max(0,round(min_val - 0.025*rango,2))
            max_val = round(max_val + 0.025*rango,2)
            
            col1, col2, col3, col4 = st.columns(4,gap="small")
            with col2:
                vmin_rango  = st.number_input('Valor mínimo eje x:',value=min_val,key='vmin_graf1')
            with col3:
                vmax_rango  = st.number_input('Valor máximo eje x:',value=max_val,key='vmax_graf1')        


        #Reemplaza nan por None
        datos_procesados             = datos_procesados.replace(numpy.nan, None)            

    
        ################# GRAFICOS ################
    
        ### GRAFICO CON LA VARIABLE ANALIZADA EN FUNCION DE LA PROFUNDIDAD Y OXIGENO   
        # Representa un gráfico con la variable seleccionada junto a los oxígenos 
        fig, (ax, az) = plt.subplots(1, 2, figsize=(20/2.54, 18/2.54), gridspec_kw = {'wspace':0.2, 'hspace':0}, width_ratios=[3, 1])
 
        ### DATOS DISPONIBLES PREVIAMENTE ###
        # Representa los datos disponibles de un color
        if io_buenos:
            ax.plot(df_datos_buenos[variable_procesada],df_datos_buenos['presion_ctd'],'.',color=color_buenos,label='BUENO')
        
        # Representa los datos dentro del intervalo de meses en otro color
        if io_rango:
            ax.plot(df_rango_temporal[variable_procesada],df_rango_temporal['presion_ctd'],'.',color=color_rango,label='BUENO (INTERVALO)')
        
        # Representa los datos con QF malos si se seleccionó esta opción   
        if io_malos:
            ax.plot(df_datos_malos[variable_procesada],df_datos_malos['presion_ctd'],'.',color=color_malos,label='MALO')    

        # Representa los datos con QF dudoso si se seleccionó esta opción   
        if io_dudosos:
            ax.plot(df_datos_dudosos[variable_procesada],df_datos_dudosos['presion_ctd'],'.',color=color_dudosos,label='DUDOSO')    

        # Representa los datos con QF No evaluado si se seleccionó esta opción   
        if io_no_eval:
            ax.plot(df_datos_no_eval[variable_procesada],df_datos_no_eval['presion_ctd'],'.',color=color_no_eval,label='NO EVALUADO')    



        ### DATOS PROCESADOS ###        
        ax.plot(datos_procesados[variable_procesada],datos_procesados['presion_ctd'],'.r',label='PROCESADO' )
        
        ### FORMATO,ETIQUETAS Y NOMBRES DE EJES ###
        texto_eje = nombre_completo_variable_procesada + '(' + unidades_variable + ')'
        ax.set(xlabel=texto_eje)
        ax.set(ylabel='Presion (db)')
        ax.invert_yaxis()
        ax.set_xlim([vmin_rango, vmax_rango])
        rango_profs = ax.get_ylim()
        # Añade el nombre de cada punto
        nombre_muestreos = [None]*datos_procesados.shape[0]
        for ipunto in range(datos_procesados.shape[0]):     
            if datos_procesados['botella'].iloc[ipunto] is None:
                nombre_muestreos[ipunto] = 'Prof.' + str(int(datos_procesados['presion_ctd'].iloc[ipunto]))
            else:
                nombre_muestreos[ipunto] = 'Bot.' + str(int(datos_procesados['botella'].iloc[ipunto]))
            ax.annotate(nombre_muestreos[ipunto], (datos_procesados[variable_procesada].iloc[ipunto], datos_procesados['presion_ctd'].iloc[ipunto]))
                
        # Ajusta el rango de las x 
        custom_ticks = numpy.linspace(vmin_rango, vmax_rango, 5, dtype=float)
        ax.set_xticks(custom_ticks)
        ax.set_xticklabels(custom_ticks)
        ax.set_xlim([vmin_rango, vmax_rango])
        ax.xaxis.set_major_formatter(FormatStrFormatter('%.2f'))   

        # Reduce el tamaño de los ejes
        ax.tick_params(axis='both', which='major', labelsize=8)

        # Añade la leyenda
        ax.legend(loc='upper center',bbox_to_anchor=(0.5, 1.15),ncol=2, fancybox=True,fontsize=7)
               
        io_plot = 0
        if not datos_procesados['oxigeno_ctd'].isnull().all(): 
            az.plot(datos_procesados['oxigeno_ctd'],datos_procesados['presion_ctd'],'.',color='#006633',label='OXIMETRO')
            io_plot = 1
                
        if not datos_procesados['oxigeno_wk'].isnull().all(): 
            az.plot(datos_procesados['oxigeno_wk'],datos_procesados['presion_ctd'],'.',color='#00CC66',label='WINKLER')
            io_plot = 1
            
        if io_plot == 1:
            az.set(xlabel='Oxigeno (\u03BCmol/kg)')
            az.yaxis.set_visible(False)
            az.invert_yaxis()
            az.set_ylim(rango_profs)
            
            # Ajusta el rango de las x
            rango_oxigenos = az.get_xlim()
            num_intervalos = 2
            val_intervalo  =  (math.ceil(rango_oxigenos[-1]) - math.floor(rango_oxigenos[0]))/num_intervalos
            az.set_xlim([math.floor(rango_oxigenos[0]),math.ceil(rango_oxigenos[-1])])
            az.set_xticks(numpy.arange(math.floor(rango_oxigenos[0]),math.ceil(rango_oxigenos[-1])+val_intervalo,val_intervalo))
            az.xaxis.set_major_formatter(FormatStrFormatter('%.0f'))
            az.tick_params(axis='both', which='major', labelsize=8)
          
            # Añade la leyenda
            az.legend(loc='upper center',bbox_to_anchor=(0.5, 1.15),ncol=1, fancybox=True,fontsize=7)

        buf = BytesIO()
        fig.savefig(buf, format="png")
        st.image(buf)

            
        #st.pyplot(fig)
 
    
 
        # ### GRAFICOS ESPECIFICOS PARA LAS POSIBLES VARIABLES        
        if variable_procesada == 'fosfato':
                        
            with st.expander("Ajustar rango del gráfico FOSFATO vs NITRATO",expanded=False):            
                
                st.write("Selecciona el rango del gráfico")  
                
                min_val_x,max_val_x = rango_datos(datos_procesados,datos_disponibles_bd,'nitrato',df_indices_calidad,io_malos,io_dudosos,io_no_eval)
               
             
                col1, col2, col3, col4 = st.columns(4,gap="small")
                with col2:
                    vmin_rango_x  = st.number_input('Valor mínimo eje x:',value=min_val_x,key='vmin_x_graf_fosf')
                with col3:
                    vmax_rango_x  = st.number_input('Valor máximo eje x:',value=max_val_x,key='vmax_x_graf_fosf')  
 

                min_val_y,max_val_y = rango_datos(datos_procesados,datos_disponibles_bd,'fosfato',df_indices_calidad,io_malos,io_dudosos,io_no_eval)
               
                
                col1, col2, col3, col4 = st.columns(4,gap="small")
                with col2:
                    vmin_rango_y  = st.number_input('Valor mínimo eje y:',value=min_val_y,key='vmin_y_graf_fosf')
                with col3:
                    vmax_rango_y  = st.number_input('Valor máximo eje y:',value=max_val_y,key='vmax_y_graf_fosf') 
            
            
    
            ### GRAFICO FOSFATO vs NITRATO 
            fig, ax = plt.subplots(figsize=(20/2.54, 18/2.54))       
            
            if io_buenos:
                ax.plot(df_datos_buenos['nitrato'],df_datos_buenos['fosfato'],'.',color=color_buenos,label='BUENO')
            
            # Representa los datos dentro del intervalo de meses en otro color
            if io_rango:
                ax.plot(df_rango_temporal['nitrato'],df_rango_temporal['fosfato'],'.',color=color_rango,label='BUENO (INTERVALO)')
            
            # Representa los datos con QF malos si se seleccionó esta opción   
            if io_malos:
                ax.plot(df_datos_malos['nitrato'],df_datos_malos['fosfato'],'.',color=color_malos,label='MALO')    

            # Representa los datos con QF dudoso si se seleccionó esta opción   
            if io_dudosos:
                ax.plot(df_datos_dudosos['nitrato'],df_datos_dudosos['fosfato'],'.',color=color_dudosos,label='DUDOSO')    
                                  
            ax.plot(datos_procesados['nitrato'],datos_procesados['fosfato'],'.r' )

            ax.set(xlabel='Nitrato (\u03BCmol/kg)')
            ax.set(ylabel='Fosfato (\u03BCmol/kg)')
    
            # Reduce el tamaño y ajusta el rango y formato de los ejes
            ax.tick_params(axis='both', which='major', labelsize=8)
            ax.set_xlim([vmin_rango_x, vmax_rango_x])
            ax.set_ylim([vmin_rango_y, vmax_rango_y])
            ax.xaxis.set_major_formatter(FormatStrFormatter('%.2f'))
            ax.yaxis.set_major_formatter(FormatStrFormatter('%.2f')) 
    
            # Añade el nombre de cada punto
            nombre_muestreos = [None]*datos_procesados.shape[0]
            for ipunto in range(datos_procesados.shape[0]):
                if datos_procesados['botella'].iloc[ipunto] is None:
                    nombre_muestreos[ipunto] = 'Prof.' + str(datos_procesados['presion_ctd'].iloc[ipunto])
                else:
                    nombre_muestreos[ipunto] = 'Bot.' + str(int(datos_procesados['botella'].iloc[ipunto]))
                ax.annotate(nombre_muestreos[ipunto], (datos_procesados['nitrato'].iloc[ipunto], datos_procesados['fosfato'].iloc[ipunto]))
           
            # st.pyplot(fig)
        
            buf = BytesIO()
            fig.savefig(buf, format="png")
            st.image(buf)        





       
        elif variable_procesada == 'nitrato':
    
            with st.expander("Ajustar rango del gráfico NITRATO vs FOSFATO",expanded=False):            
                
                st.write("Selecciona el rango del gráfico")  
                
                # Selecciona los rangos del gráfico
                min_val_x,max_val_x = rango_datos(datos_procesados,datos_disponibles_bd,'nitrato',df_indices_calidad,io_malos,io_dudosos,io_no_eval)
               
                col1, col2, col3, col4 = st.columns(4,gap="small")
                with col2:
                    vmin_rango_x_g1  = st.number_input('Valor mínimo nitrato:',value=min_val_x,key='vmin_nit')
                with col3:
                    vmax_rango_x_g1  = st.number_input('Valor máximo nitrato:',value=max_val_x,key='vmax_nit')  
 
                min_val_y,max_val_y = rango_datos(datos_procesados,datos_disponibles_bd,'fosfato',df_indices_calidad,io_malos,io_dudosos,io_no_eval)

                col1, col2, col3, col4 = st.columns(4,gap="small")
                with col2:
                    vmin_rango_y_g1  = st.number_input('Valor mínimo fosfato:',value=min_val_y,key='vmin_fosf')
                with col3:
                    vmax_rango_y_g1  = st.number_input('Valor máximo fosfato:',value=max_val_y,key='vmax_fosf')         

   
                    
            if datos_procesados['ph'].isnull().all():         
                fig, ax = plt.subplots()      
            else:
                
                with st.expander("Ajustar rango del gráfico NITRATO vs ph",expanded=False):            
                    
                    st.write("Selecciona el rango del gráfico")  
                    
                    # Selecciona los rangos del gráfico
                    min_val_x_g2,max_val_x_g2 = rango_datos(datos_procesados,datos_disponibles_bd,'nitrato',df_indices_calidad,io_malos,io_dudosos,io_no_eval)

                    col1, col2, col3, col4 = st.columns(4,gap="small")
                    with col2:
                        vmin_rango_x_g2  = st.number_input('Valor mínimo nitrato:',value=min_val_x_g2,key='vmin_nit_2')
                    with col3:
                        vmax_rango_x_g2  = st.number_input('Valor máximo nitrato:',value=max_val_x_g2,key='vmax_nit_2')  
     
                    min_val_y_g2,max_val_y_g2 = rango_datos(datos_procesados,datos_disponibles_bd,'ph',df_indices_calidad,io_malos,io_dudosos,io_no_eval)

                    col1, col2, col3, col4 = st.columns(4,gap="small")
                    with col2:
                        vmin_rango_y_g2  = st.number_input('Valor mínimo pH:',value=min_val_y_g2,key='vmin_ph')
                    with col3:
                        vmax_rango_y_g2  = st.number_input('Valor máximo pH:',value=max_val_y_g2,key='vmax_ph') 



                fig, (ax, az) = plt.subplots(1, 2, figsize=(20/2.54, 18/2.54), gridspec_kw = {'wspace':0.2, 'hspace':0}, width_ratios=[1, 1])
 
    
            ### GRAFICO FOSFATO vs NITRATO
            if io_buenos:
                ax.plot(df_datos_buenos['nitrato'],df_datos_buenos['fosfato'],'.',color=color_buenos,label='BUENO')
            
            # Representa los datos dentro del intervalo de meses en otro color
            if io_rango:
                ax.plot(df_rango_temporal['nitrato'],df_rango_temporal['fosfato'],'.',color=color_rango,label='BUENO (INTERVALO)')
            
            # Representa los datos con QF malos si se seleccionó esta opción   
            if io_malos:
                ax.plot(df_datos_malos['nitrato'],df_datos_malos['fosfato'],'.',color=color_malos,label='MALO')    

            # Representa los datos con QF dudoso si se seleccionó esta opción   
            if io_dudosos:
                ax.plot(df_datos_dudosos['nitrato'],df_datos_dudosos['fosfato'],'.',color=color_dudosos,label='DUDOSO')    
  
            ax.plot(datos_procesados['nitrato'],datos_procesados['fosfato'],'.r' )
            
            ax.set(xlabel='Nitrato (\u03BCmol/kg)')
            ax.set(ylabel='Fosfato (\u03BCmol/kg)')
            
            # Reduce el tamaño y ajusta el formato de los ejes
            ax.tick_params(axis='both', which='major', labelsize=8)
            ax.xaxis.set_major_formatter(FormatStrFormatter('%.2f')) 
            ax.yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
            ax.set_xlim([vmin_rango_x_g1, vmax_rango_x_g1])
            ax.set_ylim([vmin_rango_y_g1, vmax_rango_y_g1])
    
            # Añade el nombre de cada punto
            nombre_muestreos = [None]*datos_procesados.shape[0]
            for ipunto in range(datos_procesados.shape[0]):
                if datos_procesados['botella'].iloc[ipunto] is None:
                    nombre_muestreos[ipunto] = 'Prof.' + str(datos_procesados['presion_ctd'].iloc[ipunto])
                else:
                    nombre_muestreos[ipunto] = 'Bot.' + str(int(datos_procesados['botella'].iloc[ipunto]))
                ax.annotate(nombre_muestreos[ipunto], (datos_procesados['nitrato'].iloc[ipunto], datos_procesados['fosfato'].iloc[ipunto]))
    
            st.dataframe(datos_procesados)
            if datos_procesados['ph'].isnull().all() is False: 
                ### GRAFICO NITRATO vs pH
            
                if io_buenos:
                    az.plot(df_datos_buenos['nitrato'],df_datos_buenos['pH'],'.',color=color_buenos,label='BUENO')
                
                # Representa los datos dentro del intervalo de meses en otro color
                if io_rango:
                    az.plot(df_rango_temporal['nitrato'],df_rango_temporal['pH'],'.',color=color_rango,label='BUENO (INTERVALO)')
                
                # Representa los datos con QF malos si se seleccionó esta opción   
                if io_malos:
                    az.plot(df_datos_malos['nitrato'],df_datos_malos['pH'],'.',color=color_malos,label='MALO')    
    
                # Representa los datos con QF dudoso si se seleccionó esta opción   
                if io_dudosos:
                    az.plot(df_datos_dudosos['nitrato'],df_datos_dudosos['pH'],'.',color=color_dudosos,label='DUDOSO')    
                                      
        
                az.plot(datos_procesados['nitrato'],datos_procesados['ph'],'.',color='#C0C0C0')
     
        
                # az.set(xlabel='Nitrato (\u03BCmol/kg)')
                # az.set(ylabel='pH')
                # az.yaxis.tick_right()
                # az.yaxis.set_label_position("right") 
                
                # az.tick_params(axis='both', which='major', labelsize=8)
                # az.xaxis.set_major_formatter(FormatStrFormatter('%.2f')) 
                # az.yaxis.set_major_formatter(FormatStrFormatter('%.3f'))
                # az.set_xlim([vmin_rango_x_g2, vmax_rango_x_g2])
                # az.set_ylim([vmin_rango_y_g2, vmax_rango_y_g2])
            
            
                # Añade el nombre de cada punto
                nombre_muestreos = [None]*datos_procesados.shape[0]
                for ipunto in range(datos_procesados.shape[0]):
                    if datos_procesados['botella'].iloc[ipunto] is None:
                        nombre_muestreos[ipunto] = 'Prof.' + str(datos_procesados['presion_ctd'].iloc[ipunto])
                    else:
                        nombre_muestreos[ipunto] = 'Bot.' + str(int(datos_procesados['botella'].iloc[ipunto]))
                    az.annotate(nombre_muestreos[ipunto], (datos_procesados['nitrato'].iloc[ipunto], datos_procesados['ph'].iloc[ipunto]))
         
    
#            st.pyplot(fig)
 
            buf = BytesIO()
            fig.savefig(buf, format="png")
            st.image(buf)      
        
      
        
      
        
        
        # # Gráficos particulares para cada variable
        # elif variable_seleccionada == 'silicato':
    
        #     if df_seleccion['silicato'].isnull().all() is False:         
    
        #         ### GRAFICO SILICATO vs ALCALINIDAD  
                
        #         with st.expander("Ajustar rango del gráfico SILICATO vs ALCALINIDAD",expanded=False):            
                    
        #             st.write("Selecciona el rango del gráfico")  
                    
        #             # Selecciona los rangos del gráfico
        #             min_val_x = 0.95*min(df_disponible_bd['silicato'].min(),df_seleccion['silicato'].min())
        #             max_val_x = 1.05*max(df_disponible_bd['silicato'].max(),df_seleccion['silicato'].max())
                       
        #             col1, col2, col3, col4 = st.columns(4,gap="small")
        #             with col2:
        #                 vmin_rango_x  = st.number_input('Valor mínimo silicato:',value=min_val_x,key='vmin_sil')
        #             with col3:
        #                 vmax_rango_x  = st.number_input('Valor máximo silicato:',value=max_val_x,key='vmax_sil')  
     
        #             min_val_y = 0.95*min(df_disponible_bd['alcalinidad'].min(),df_seleccion['alcalinidad'].min())
        #             max_val_y = 1.05*max(df_disponible_bd['alcalinidad'].max(),df_seleccion['alcalinidad'].max())
                       
        #             col1, col2, col3, col4 = st.columns(4,gap="small")
        #             with col2:
        #                 vmin_rango_y  = st.number_input('Valor mínimo alcalinidad:',value=min_val_y,key='vmin_alc')
        #             with col3:
        #                 vmax_rango_y  = st.number_input('Valor máximo alcalinidad:',value=max_val_y,key='vmax_alc')   
    
        #         fig, ax = plt.subplots()       
                
        #         if io_buenos:
        #             ax.plot(df_datos_buenos['silicato'],df_datos_buenos['alcalinidad'],'.',color=color_buenos,label='BUENO')
                
        #         # Representa los datos dentro del intervalo de meses en otro color
        #         if io_rango:
        #             ax.plot(df_rango_temporal['silicato'],df_rango_temporal['alcalinidad'],'.',color=color_rango,label='BUENO (INTERVALO)')
                
        #         # Representa los datos con QF malos si se seleccionó esta opción   
        #         if io_malos:
        #             ax.plot(df_datos_malos['silicato'],df_datos_malos['alcalinidad'],'.',color=color_malos,label='MALO')    
    
        #         # Representa los datos con QF dudoso si se seleccionó esta opción   
        #         if io_dudosos:
        #             ax.plot(df_datos_dudosos['silicato'],df_datos_dudosos['alcalinidad'],'.',color=color_dudosos,label='DUDOSO')    
                                
                
        #         ax.plot(df_seleccion['silicato'],df_seleccion['alcalinidad'],'.r' )
                
                
        #         ax.set(xlabel='Silicato (\u03BCmol/kg)')
        #         ax.set(ylabel='Alcalinidad (\u03BCmol/kg)')
                
        #         ax.tick_params(axis='both', which='major', labelsize=8)
        #         ax.xaxis.set_major_formatter(FormatStrFormatter('%.2f')) 
        #         ax.yaxis.set_major_formatter(FormatStrFormatter('%.0f'))
        #         ax.set_xlim([vmin_rango_x, vmax_rango_x])
        #         ax.set_ylim([vmin_rango_y, vmax_rango_y])
        
        #         # Añade el nombre de cada punto
        #         nombre_muestreos = [None]*df_seleccion.shape[0]
        #         for ipunto in range(df_seleccion.shape[0]):
        #             if df_seleccion['botella'].iloc[ipunto] is None:
        #                 nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
        #             else:
        #                 nombre_muestreos[ipunto] = 'Bot.' + str(int(df_seleccion['botella'].iloc[ipunto]))
        #             ax.annotate(nombre_muestreos[ipunto], (df_seleccion['silicato'].iloc[ipunto], df_seleccion['alcalinidad'].iloc[ipunto]))
               
        #         st.pyplot(fig)
    
    
        ################# FORMULARIOS CALIDAD ################        
    
        # Formulario para asignar banderas de calidad
        with st.form("Formulario", clear_on_submit=False):
                      
            indice_validacion = df_indices_calidad['indice'].tolist()
            texto_indice      = df_indices_calidad['descripcion'].tolist()
           
            for idato in range(datos_procesados.shape[0]):
                enunciado                                           = 'QF del muestreo ' + nombre_muestreos[idato]
                valor_asignado                                      = st.radio(enunciado,texto_indice,horizontal=True,key = idato,index = 1)
                datos_procesados[qf_variable_procesada].iloc[idato] = int(indice_validacion[texto_indice.index(valor_asignado)])
            
           
            io_envio = st.form_submit_button("Añadir resultados a la base de datos con los índices seleccionados")  
    
            if io_envio:
            
                direccion_host   = st.secrets["postgres"].host
                base_datos       = st.secrets["postgres"].dbname
                usuario          = st.secrets["postgres"].user
                contrasena       = st.secrets["postgres"].password
                puerto           = st.secrets["postgres"].port    
            
                with st.spinner('Actualizando la base de datos'):
               
                    # Introducir los valores en la base de datos
                    conn   = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                    cursor = conn.cursor()  
                    instruccion_sql = "UPDATE " + tabla_insercion + " SET " + qf_variable_procesada +  ' = %s WHERE muestreo = %s;'
           
                    for idato in range(datos_procesados.shape[0]):
        
                        cursor.execute(instruccion_sql, (int(datos_procesados[qf_variable_procesada].iloc[idato]),int(datos_procesados['muestreo'].iloc[idato])))
                        conn.commit() 
        
                    cursor.close()
                    conn.close()   
    
                texto_exito = 'QF de la ' + nombre_completo_variable_procesada + ' asignadas o modificadas correctamente'
                st.success(texto_exito)
   












################################################################
######## FUNCION PARA REALIZAR LA CORRECCIÓN DE DERIVA  ########
################################################################

def correccion_drift(datos_entrada,df_referencias,variables_run,rendimiento_columna,temperatura_laboratorio):
    
    # Predimensiona un dataframe con los resultados de la correccion
    datos_corregidos = pandas.DataFrame(columns=variables_run)    

    # Encuentra los índices (picos) correspondientes a la calbración
    indices_calibracion = numpy.asarray(datos_entrada['Peak Number'][datos_entrada['Cup Type']=='CALB']) - 1
           
    # Corrige las concentraciones a partir de los rendimientos de la coumna reductora
    datos_entrada['nitrato_rendimiento'] = numpy.zeros(datos_entrada.shape[0])
    datos_entrada['ton_rendimiento'] = numpy.zeros(datos_entrada.shape[0])
    factor = ((datos_entrada['ton'].iloc[indices_calibracion[-1]]*rendimiento_columna/100) + datos_entrada['nitrito'].iloc[indices_calibracion[-1]])/(datos_entrada['ton'].iloc[indices_calibracion[-1]] + datos_entrada['nitrito'].iloc[indices_calibracion[-1]])
    for idato in range(datos_entrada.shape[0]):
        datos_entrada['nitrato_rendimiento'].iloc[idato] = (datos_entrada['ton'].iloc[idato]*factor - datos_entrada['nitrito'].iloc[idato])/(rendimiento_columna/100) 
        datos_entrada['ton_rendimiento'].iloc[idato] = datos_entrada['nitrato_rendimiento'].iloc[idato] + datos_entrada['nitrito'].iloc[idato]
    
    # Asocia la temperatura de laboratorio a todas las muestras
    datos_entrada['temp.lab'] = temperatura_laboratorio
    
    # Pasa las concentraciones a mol/kg
    posicion_RMN_bajos = numpy.zeros(2,dtype=int)
    posicion_RMN_altos = numpy.zeros(2,dtype=int)
    icont_bajos        = 0
    icont_altos        = 0

    for idato in range(datos_entrada.shape[0]):
        if datos_entrada['Sample ID'].iloc[idato][0:7].lower() == 'rmn low' :
            posicion_RMN_bajos[icont_bajos] = idato
            icont_bajos                     = icont_bajos + 1 
            datos_entrada['salinidad'].iloc[idato]  = df_referencias['salinidad_rmn_bajo'][0]
        if datos_entrada['Sample ID'].iloc[idato][0:8].lower() == 'rmn high':
            posicion_RMN_altos[icont_altos] = idato
            icont_altos                     = icont_altos + 1
            datos_entrada['salinidad'].iloc[idato]  = df_referencias['salinidad_rmn_alto'][0]

    densidades = seawater.eos80.dens0(datos_entrada['salinidad'], datos_entrada['temp.lab'])
    datos_entrada['DENSIDAD'] = densidades/1000  
                    
    datos_entrada['ton_CONC'] = datos_entrada['ton_rendimiento']/datos_entrada['DENSIDAD']  
    datos_entrada['nitrato_CONC'] = datos_entrada['nitrato_rendimiento']/datos_entrada['DENSIDAD']  
    datos_entrada['nitrito_CONC'] = datos_entrada['nitrito']/datos_entrada['DENSIDAD']  
    datos_entrada['silicato_CONC'] = datos_entrada['silicato']/datos_entrada['DENSIDAD']  
    datos_entrada['fosfato_CONC'] = datos_entrada['fosfato']/datos_entrada['DENSIDAD']  


    
    ####  APLICA LA CORRECCIÓN DE DERIVA ####
    # Encuentra las posiciones de los RMNs
    #posicion_RMN_bajos  = [i for i, e in enumerate(datos_entrada['Sample ID']) if e == 'RMN Low']
    #posicion_RMN_altos  = [i for i, e in enumerate(datos_entrada['Sample ID']) if e == 'RMN High']
    
    for ivariable in range(len(variables_run)):
               
        variable_concentracion  = variables_run[ivariable] + '_CONC'
        
        # Concentraciones de las referencias
        #variable_rmn    = variables_run[ivariable] + '_rmn_bajo'
        # RMN_CE_variable = df_referencias[variables_run[ivariable]].iloc[0]
        # RMN_CI_variable = df_referencias[variables_run[ivariable]].iloc[1]  
        RMN_CE_variable = df_referencias[variables_run[ivariable] + '_rmn_bajo'].iloc[0]
        RMN_CI_variable = df_referencias[variables_run[ivariable] + '_rmn_alto'].iloc[0]  
        
        # Concentraciones de las muestras analizadas como referencias
        RMN_altos       = datos_entrada[variable_concentracion][posicion_RMN_altos]
        RMN_bajos       = datos_entrada[variable_concentracion][posicion_RMN_bajos]
            
        # Predimensiona las rectas a y b
        indice_min_correccion = min(posicion_RMN_altos[0],posicion_RMN_bajos[0])
        indice_max_correccion = max(posicion_RMN_altos[1],posicion_RMN_bajos[1])
        recta_at              = numpy.zeros(datos_entrada.shape[0])
        recta_bt              = numpy.zeros(datos_entrada.shape[0])
            
        pte_RMN      = (RMN_CI_variable-RMN_CE_variable)/(RMN_altos.iloc[0]-RMN_bajos.iloc[0]) 
        t_indep_RMN  = RMN_CE_variable- pte_RMN*RMN_bajos.iloc[0] 
    
        variable_drift = numpy.zeros(datos_entrada.shape[0])
    
        # Aplica la corrección basada de dos rectas, descrita en Hassenmueller
        for idato in range(indice_min_correccion,indice_max_correccion):

            factor_f        = (idato-posicion_RMN_bajos[0])/(posicion_RMN_bajos[1]-posicion_RMN_bajos[0])
            recta_at[idato] = RMN_bajos.iloc[0] +  factor_f*(RMN_bajos.iloc[-1]-RMN_bajos.iloc[0]) 
            
            factor_f        = (idato-posicion_RMN_altos[0])/(posicion_RMN_altos[1]-posicion_RMN_altos[0])
            recta_bt[idato] = RMN_altos.iloc[0] +  factor_f*(RMN_altos.iloc[-1]-RMN_altos.iloc[0]) 
    
            val_combinado   = ((datos_entrada[variable_concentracion][idato]-recta_at[idato])/(recta_bt[idato]-recta_at[idato]))*(RMN_altos.iloc[0]-RMN_bajos.iloc[0]) + RMN_bajos.iloc[0]
                 
            variable_drift[idato] = val_combinado*pte_RMN+t_indep_RMN
    
        variable_drift[variable_drift<0] = 0
        
        datos_corregidos[variables_run[ivariable]] = variable_drift
        
    # Añade columna con el identificador de cada muestra
    datos_corregidos['nombre_muestreo'] = datos_entrada['Sample ID']
       
    return datos_corregidos
    
################################################################
######## FUNCION PARA ASOCIAR A CADA DATO EL FACTOR CORRECTOR DE QC2 DE NUTRIENTES ########
################################################################    

def recupera_factores_nutrientes(df_muestreos_seleccionados):
    
    import streamlit as st
    from FUNCIONES.FUNCIONES_AUXILIARES import menu_seleccion   
    from FUNCIONES.FUNCIONES_AUXILIARES import init_connection 
    
    # Recupera los datos de conexión
    direccion_host   = st.secrets["postgres"].host
    base_datos       = st.secrets["postgres"].dbname
    usuario          = st.secrets["postgres"].user
    contrasena       = st.secrets["postgres"].password
    puerto           = st.secrets["postgres"].port

    # Recupera información de la base de datos
    conn                      = init_connection()
    df_factores_nutrientes    = psql.read_sql('SELECT * FROM factores_correctores_nutrientes', conn)
    df_muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
    conn.close()


    # Carga la tabla con los factores de corrección
    
    
    
    return df_muestreos_seleccionados