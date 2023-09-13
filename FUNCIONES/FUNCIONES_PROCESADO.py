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
from io import BytesIO

from matplotlib.ticker import FormatStrFormatter

pandas.options.mode.chained_assignment = None

#######################################################################################
######## FUNCION PARA APLICAR CONTROL DE CALIDAD BÁSICO LOS DATOS DE PROGRAMAS  #######
#######################################################################################
def control_calidad(datos):
 
    textos_aviso = [] 
      
    # Cambia valores no válidos por None
    datos = datos.replace({numpy.nan:None})
    datos = datos.replace(-999, None) 
    
    listado_variables_datos   = datos.columns.tolist()
           
    # # Elimina datos sin profundidad o fecha (en caso de que estas variables estén entre las de entrada)
    # if 'presion_ctd' in listado_variables_datos :        
    #     datos = datos[datos['presion_ctd'].notna()]
    #     datos = datos.drop(datos[datos.presion_ctd < 0].index) # Corregir los valores positivos de longitud, pasándolos a negativos (algunos datos de Pelacus tienen este error)
    # if 'fecha_muestreo' in listado_variables_datos:        
    #     datos = datos[datos['fecha_muestreo'].notna()] 
        
    
    # # Elimina registros duplicados en el mismo punto y a la misma hora(por precaucion)
    if 'latitud' in listado_variables_datos and 'longitud' in listado_variables_datos and 'presion_ctd' in listado_variables_datos and 'fecha_muestreo' in listado_variables_datos and 'hora_muestreo' in listado_variables_datos : 
        num_reg_inicial = datos.shape[0]
        datos           = datos.drop_duplicates(subset=['latitud','longitud','presion_ctd','fecha_muestreo','hora_muestreo'], keep='last')    
        num_reg_final   = datos.shape[0]
        if num_reg_final < num_reg_inicial:
            textos_aviso.append('Se han eliminado registros correspondientes a una misma fecha, hora, profundidad y estación')
    
    # Define un nuevo índice de filas. Si se han eliminado registros este paso es necesario
    indices_dataframe        = numpy.arange(0,datos.shape[0],1,dtype=int)    
    datos['id_temp'] = indices_dataframe
    datos.set_index('id_temp',drop=False,append=False,inplace=True)
    
    # Redondea los decimales los datos de latitud, longitud y profundidad (precisión utilizada en la base de datos) 
    if 'longitud' in listado_variables_datos : 
        datos['longitud'] = -1*datos['longitud'].abs()  
        for idato in range(datos.shape[0]):
            datos['longitud'][idato] = round(datos['longitud'][idato],4)
    
    if 'latitud' in listado_variables_datos:
        for idato in range(datos.shape[0]):
            datos['latitud'][idato] = round(datos['latitud'][idato],4)
        
    if 'presion_ctd' in listado_variables_datos:
        for idato in range(datos.shape[0]):
            if datos['presion_ctd'][idato] is not None:
                datos['presion_ctd'][idato] = round(float(datos['presion_ctd'][idato]),2)      

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
    
    listado_variables = datos.columns.tolist()
    if 'latitud' not in listado_variables:
        datos['latitud'] = [None]*datos.shape[0]
    if 'longitud' not in listado_variables:
        datos['longitud'] = [None]*datos.shape[0]   
        
    # Genera un dataframe con las estaciones incluidas en el muestreo
    estaciones_muestreadas                      = datos['estacion'].unique()
    estaciones_muestreadas                      = pandas.DataFrame(data=estaciones_muestreadas,columns=['nombre_estacion'])  
    estaciones_muestreadas['id_estacion']       = numpy.zeros(estaciones_muestreadas.shape[0],dtype=int)
    estaciones_muestreadas['io_nueva_estacion'] = numpy.zeros(estaciones_muestreadas.shape[0],dtype=int)
    estaciones_muestreadas['latitud_estacion']  = [None]*estaciones_muestreadas.shape[0]
    estaciones_muestreadas['longitud_estacion'] = [None]*estaciones_muestreadas.shape[0]


    # Contadores e identificadores 
    if len(tabla_estaciones['id_estacion'])>0:
        id_ultima_estacion_bd = max(tabla_estaciones['id_estacion'])
    else:
        id_ultima_estacion_bd = 0
        
    iconta_nueva_estacion     = 1
    
    # Encuentra el identificador asociado a cada estacion en la base de datos
    for iestacion in range(estaciones_muestreadas.shape[0]):
        
        df_temporal = estaciones_programa[estaciones_programa['nombre_estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]]
    
        #print(df_temporal)
    
        # Estacion ya incluida en la base de datos. Recuperar identificador
        if df_temporal.shape[0]>0:
            estaciones_muestreadas['id_estacion'][iestacion]       = df_temporal['id_estacion'].iloc[0]
            
            # Asigna lat/lon a la medida si ésta no la tenía
            if 'latitud' not in listado_variables:
                datos['latitud'][datos['estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]] =  df_temporal['latitud_estacion'].iloc[0] 
            if 'longitud' not in listado_variables:
                datos['longitud'][datos['estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]] =  df_temporal['longitud_estacion'].iloc[0] 
            
             
        # Nueva estación, asignar orden creciente de identificador
        else:
            estaciones_muestreadas['id_estacion'][iestacion]       = id_ultima_estacion_bd + iconta_nueva_estacion
            estaciones_muestreadas['io_nueva_estacion'][iestacion] = 1           
            iconta_nueva_estacion                                 = iconta_nueva_estacion + 1
  
            # Determina la lat/lon de la estacion a partir de los valores de los registros asociados
            if 'latitud' in listado_variables:
                estaciones_muestreadas['latitud_estacion'][iestacion]  = (datos['latitud'][datos['estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]]).mean()
            if 'longitud' in listado_variables:
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
    
#    # Cambia el nombre de la columna "estacion" por "nombre_estacion"
#    datos = datos.rename(columns={"estacion":"nombre_estacion"})
    
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
    
    if tipo_salida == 'PUNTUAL' and id_programa == 6: # Muestra puntual de un programa que no es estructural
    
        dias_salida_mar = datos['fecha_muestreo'].unique()
        
        for idia in range(len(dias_salida_mar)):    
      
            df_temporal = tabla_salidas[(tabla_salidas['fecha_salida']==dias_salida_mar[idia]) & (tabla_salidas['programa']==id_programa)]
    
            # Salida ya incluida en la base de datos. Recuperar identificador
            if df_temporal.shape[0]>0:
                id_salida = df_temporal['id_salida'].iloc[0]
                
            # Salida no incluida. Añadirla a la base de datos.
            else:
            
                id_salida           = id_ultima_salida_bd + iconta_nueva_salida
                iconta_nueva_salida = iconta_nueva_salida + 1
            
                # Encuentra las estaciones muestreadas
                subset_salida                        = datos[datos['fecha_muestreo']==dias_salida_mar[idia]]
                identificador_estaciones_muestreadas = list(subset_salida['id_estacion_temp'].unique())
                estaciones_muestreadas               =[None]*len(identificador_estaciones_muestreadas)
                for iestacion in range(len(estaciones_muestreadas)):
                    estaciones_muestreadas[iestacion] = tabla_estaciones['nombre_estacion'][tabla_estaciones['id_estacion']==identificador_estaciones_muestreadas[iestacion]].iloc[0]
                json_estaciones        = json.dumps(estaciones_muestreadas)
                   
                # Define nombre
                nombre_salida = nombre_programa + ' ' + str(dias_salida_mar[idia])
          
                # Inserta en la base de datos
                conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                cursor = conn.cursor()                      
                instruccion_sql = '''INSERT INTO salidas_muestreos (id_salida,nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO NOTHING;'''        
                cursor.execute(instruccion_sql, (int(id_salida),nombre_salida,int(id_programa),nombre_programa,tipo_salida,dias_salida_mar[idia],dias_salida_mar[idia],json_estaciones))
                conn.commit()
                cursor.close()
                conn.close()
                
            # Asigna el id de la salida al dataframe
            datos['id_salida'][datos['fecha_muestreo']==dias_salida_mar[idia]] = id_salida
        
        
        
        


    if tipo_salida == 'ANUAL':

        
        # Añade una columna a las salidas con el año
        tabla_salidas['año_salida']= numpy.zeros(tabla_salidas.shape[0])
        for idato in range(tabla_salidas.shape[0]):
            tabla_salidas['año_salida'].iloc[idato] =  tabla_salidas['fecha_salida'].iloc[idato].year    
            

        anhos_salida_mar = datos['año'].unique()
        
        for ianho in range(len(anhos_salida_mar)):    
 
            subset_anual     = datos[datos['año']==anhos_salida_mar[ianho]] 
            
            df_temporal = tabla_salidas[(tabla_salidas['año_salida']==anhos_salida_mar[ianho]) & (tabla_salidas['programa']==id_programa)]
    
            # Salida ya incluida en la base de datos. Recuperar identificador
            if df_temporal.shape[0]>0:
                id_salida = df_temporal['id_salida'].iloc[0]
                
            # Salida no incluida. Añadirla a la base de datos.
            else:
                
                # Busca las fechas de salida y llegada
                fechas_anuales   = subset_anual['fecha_muestreo'].unique()
                fecha_salida     = min(fechas_anuales)
                fecha_llegada    = max(fechas_anuales)  
            
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
                nombre_salida = nombre_programa + ' ' + str(int(anhos_salida_mar[ianho]))
          
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
    
            
    if tipo_salida == 'MENSUAL' or tipo_salida == 'SEMANAL' : # Programas radiales

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
    
                #df_temporal = tabla_salidas[tabla_salidas['fecha_salida']==fechas_partida[isalida]]
                df_temporal = tabla_salidas[(tabla_salidas['fecha_salida']==fechas_partida[isalida]) & (tabla_salidas['programa']==id_programa)]
        
                # Salida ya incluida en la base de datos. Recuperar identificador
                if df_temporal.shape[0]>0:
                    id_salida = df_temporal['id_salida'].iloc[0]
                    
                # Salida no incluida. Añadirla a la base de datos.
                else:
                
                    id_salida           = id_ultima_salida_bd + iconta_nueva_salida
                    iconta_nueva_salida = iconta_nueva_salida + 1
                
                    # Encuentra las estaciones muestreadas
                    #subset_salida                        = datos[datos['fecha_muestreo']==fechas_salidas_mar[isalida]]
                    
                    subset_salida = datos[(datos['fecha_muestreo']>=fechas_partida[isalida]) & (datos['fecha_muestreo']<=fechas_regreso[isalida])]
                    
                    
                    
                    identificador_estaciones_muestreadas = list(subset_salida['id_estacion_temp'].unique())
                    estaciones_muestreadas               =[None]*len(identificador_estaciones_muestreadas)
                    for iestacion in range(len(estaciones_muestreadas)):
                        estaciones_muestreadas[iestacion] = tabla_estaciones['nombre_estacion'][tabla_estaciones['id_estacion']==identificador_estaciones_muestreadas[iestacion]].iloc[0]
                    json_estaciones        = json.dumps(estaciones_muestreadas)
                       
                    # Define nombre
                    if tipo_salida == 'MENSUAL':
                        nombre_salida = nombre_programa + ' ' + tipo_salida + ' ' +   str(meses[fechas_partida[isalida].month-1]) + ' ' +  str(fechas_partida[isalida].year)
                    if tipo_salida == 'SEMANAL':   
                        nombre_salida = nombre_programa + ' ' + tipo_salida + ' ' +   str(meses[fechas_partida[isalida].month-1]) + ' ' +  str(fechas_partida[isalida].year) + ' SEMANA ' + str(round(fechas_partida[isalida].day/7)+1)
                    # Inserta en la base de datos
                    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                    cursor = conn.cursor()                      
                    instruccion_sql = '''INSERT INTO salidas_muestreos (id_salida,nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO NOTHING;'''        
                    cursor.execute(instruccion_sql, (int(id_salida),nombre_salida,int(id_programa),nombre_programa,tipo_salida,fechas_partida[isalida],fechas_regreso[isalida],json_estaciones))
                    conn.commit()
                    cursor.close()
                    conn.close()
                    
                # Asigna el id de la salida al dataframe
                if id_programa == 3:
                    datos['id_salida'][datos['fecha_muestreo']==fechas_salidas_mar[isalida]] = id_salida
                if id_programa == 4:                
                    #datos['id_salida'][(datos['año']==anhos_salida_mar[ianho]) & (datos['mes']==fechas_partida[isalida].month)] = id_salida
                    datos['id_salida'][(datos['fecha_muestreo']>=fechas_partida[isalida]) & (datos['fecha_muestreo']<=fechas_regreso[isalida])]  = id_salida
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
    tabla_variables  = psql.read_sql('SELECT * FROM variables_procesado', conn_psql)
    conn_psql.dispose() 
    
    listado_variables_datos   = datos.columns.tolist()
    
    df_variables = tabla_variables[tabla_variables['tipo']=='parametro_muestreo']
    
    datos['muestreo']  = numpy.zeros(datos.shape[0],dtype=int)
    
    # si no hay ningun valor en la tabla de registro, meter directamente todos los datos registrados
    if tabla_muestreos.shape[0] == 0:
    
        # Mantén sólo las columnas que interesan
        
        
        #variables_bd  = [x for x in tabla_variables['parametros_muestreo'] if str(x) != 'None']
        
        # Busca qué variables están incluidas en los datos a importar
        listado_variables_comunes = list(set(listado_variables_datos).intersection(df_variables['nombre']))
        listado_adicional         = ['id_estacion_temp','id_salida'] + listado_variables_comunes
        exporta_registros         = datos[listado_adicional]

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
            if 'num_cast' in listado_variables_datos and datos['num_cast'].iloc[idato] is not None:
                nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'].iloc[idato]))
            else:
                nombre_muestreo = nombre_muestreo + '_C1' 
                
            if 'botella' in listado_variables_datos and datos['botella'].iloc[idato] is not None:
                nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'].iloc[idato])) 
            else:
                if 'prof_referencia' in listado_variables_datos and datos['prof_referencia'].iloc[idato] is not None: 
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

        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
        cursor = conn.cursor()
        
        for idato in range(datos.shape[0]):
            
            if 'nombre_muestreo' in listado_variables_datos and datos['nombre_muestreo'].iloc[idato] is not None: 
                
                df_temp = tabla_muestreos[(tabla_muestreos['nombre_muestreo']==datos['nombre_muestreo'].iloc[idato])]
                
            else:
                
                if 'botella' in listado_variables_datos and datos['botella'].iloc[idato] is not None:        
                    if 'hora_muestreo' in listado_variables_datos and datos['hora_muestreo'].iloc[idato] is not None:          
                        df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['botella']==datos['botella'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) & (tabla_muestreos['hora_muestreo']==datos['hora_muestreo'].iloc[idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]
    
                    else:
                        df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['botella']==datos['botella'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) ]#& (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]
                else:
                    if 'hora_muestreo' in listado_variables_datos and datos['hora_muestreo'].iloc[idato] is not None:          
                        df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) & (tabla_muestreos['hora_muestreo']==datos['hora_muestreo'].iloc[idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]   
                    else:
                        df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'].iloc[idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'].iloc[idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'].iloc[idato])]
                
            if df_temp.shape[0]> 0:
                datos['muestreo'].iloc[idato]          = df_temp['muestreo'].iloc[0]    
                datos['io_nuevo_muestreo'].iloc[idato] = 0
                
                if 'id_externo' in listado_variables_datos and datos['id_externo'].iloc[idato] is not None:
                    # Inserta en la base de datos
                      
                    instruccion_sql = 'UPDATE muestreos_discretos SET id_externo =%s WHERE muestreo = %s;'
                    cursor.execute(instruccion_sql, (datos['id_externo'].iloc[idato],int(datos['muestreo'].iloc[idato])))
                    conn.commit()
                                
            else:
                datos['io_nuevo_muestreo'].iloc[idato] = 1
                ultimo_registro_bd                     = ultimo_registro_bd + 1
                datos['muestreo'].iloc[idato]       = ultimo_registro_bd 
            
        cursor.close()
        conn.close()
           
        
        if numpy.count_nonzero(datos['io_nuevo_muestreo']) > 0:
        
            # Genera un dataframe sólo con los valores nuevos, a incluir (io_nuevo_muestreo = 1)
            nuevos_muestreos  = datos[datos['io_nuevo_muestreo']==1]
            
            # Mantén sólo las columnas que interesan
            
            # Busca qué variables están incluidas en los datos a importar
            listado_variables_comunes = list(set(listado_variables_datos).intersection(df_variables['nombre']))
            listado_adicional         = ['muestreo','id_estacion_temp','id_salida'] + listado_variables_comunes
            exporta_registros         = nuevos_muestreos[listado_adicional]
             
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
                if 'num_cast' in listado_variables_datos and datos['num_cast'].iloc[idato] is not None:
                    nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'].iloc[idato]))
                else:
                    nombre_muestreo = nombre_muestreo + '_C1'
                
                if 'botella' in listado_variables_datos and datos['botella'].iloc[idato] is not None:
                    nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'].iloc[idato])) 
                else:
                    if 'prof_referencia' in listado_variables_datos and datos['prof_referencia'].iloc[idato] is not None: 
                        nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['prof_referencia'].iloc[idato]))
                    else:
                        nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['presion_ctd'].iloc[idato])) 
                 
                exporta_registros['nombre_muestreo'].iloc[idato]  = nombre_muestreo
        
            # # Inserta el dataframe resultante en la base de datos 
            conn_psql        = create_engine(con_engine)
            exporta_registros.set_index('muestreo',drop=True,append=False,inplace=True)
            exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append')    
            conn_psql.dispose() # Cierra la conexión con la base de datos  
    
    return datos


############################################################################
######## FUNCION PARA INSERTAR DATOS DISCRETOS EN LA BASE DE DATOS  ########
############################################################################

def inserta_datos(datos_insercion,tipo_datos,direccion_host,base_datos,usuario,contrasena,puerto):
  
    if tipo_datos     == 'discreto':
        tabla_destino = 'datos_discretos'
        puntero       = 'muestreo'
    
    elif tipo_datos   == 'perfil':
        tabla_destino = 'datos_perfiles'
        puntero       = 'perfil'            
                    
    # Recupera la tabla con los registros de muestreos físicos
    con_engine         = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql          = create_engine(con_engine)
    tabla_variables    = psql.read_sql('SELECT * FROM variables_procesado', conn_psql) 
    instruccion_sql    = 'SELECT * FROM ' + tabla_destino
    tabla_registros    = psql.read_sql(instruccion_sql, conn_psql)
    conn_psql.dispose()
        
    # Lee las variables de cada tipo a utilizar en el control de calidad
    df_variables = tabla_variables[tabla_variables['tipo']=='variable_muestreo']
    variables_bd = df_variables['nombre']    
  
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
def control_calidad_biogeoquimica(datos_procesados,datos_disponibles_bd,variable_procesada,nombre_completo_variable_procesada,unidades_variable,df_indices_calidad,meses_offset):

    import streamlit as st
    import matplotlib.pyplot as plt
    
    def rango_datos(datos_procesados,datos_disponibles_bd,variable_procesada,df_indices_calidad,io_malos,io_dudosos,io_no_eval):

        if variable_procesada == 'ph' or variable_procesada == 'alcalinidad':
            fmin                      = 0.995
            fmax                      = 1.005         
        else:
            fmin                      = 0.95
            fmax                      = 1.05       

        id_dato_malo              = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Malo'].iloc[0]
        id_dato_bueno             = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Bueno'].iloc[0]
        id_dato_dudoso            = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Dudoso'].iloc[0]
        id_dato_no_eval           = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='No evaluado'].iloc[0]
        
        min_seleccion = min(value for value in datos_procesados[variable_procesada] if value is not None)
        max_seleccion = max(value for value in datos_procesados[variable_procesada] if value is not None)
        # min_seleccion = numpy.nanmin(numpy.array(datos_procesados[variable_procesada]))
        # max_seleccion = numpy.nanmax(numpy.array(datos_procesados[variable_procesada]))
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
            df_datos_malos = datos_disponibles_bd[datos_disponibles_bd[qf_variable_procesada]==id_dato_malo]
            if df_datos_malos.shape[0] > 0:
                min_bd         = numpy.nanmin(numpy.array(df_datos_malos[variable_procesada]))
                max_bd         = numpy.nanmax(numpy.array(df_datos_malos[variable_procesada]))
                min_val        = fmin*min(min_val,min_seleccion)
                max_val        = fmax*max(max_val,max_seleccion)  
    
        if io_dudosos:
            df_datos_dudosos = datos_disponibles_bd[datos_disponibles_bd[qf_variable_procesada]==id_dato_dudoso]
            if df_datos_dudosos.shape[0] > 0:
                min_bd           = numpy.nanmin(numpy.array(df_datos_dudosos[variable_procesada]))
                max_bd           = numpy.nanmax(numpy.array(df_datos_dudosos[variable_procesada]))
                min_val          = fmin*min(min_val,min_seleccion)
                max_val          = fmax*max(max_val,max_seleccion) 
            
        if io_no_eval:
            df_datos_no_eval = datos_disponibles_bd[datos_disponibles_bd[qf_variable_procesada]==id_dato_no_eval]
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

    df_datos_no_eval = datos_disponibles_bd[datos_disponibles_bd[qf_variable_procesada]==id_dato_no_eval]
    df_datos_malos = datos_disponibles_bd[datos_disponibles_bd[qf_variable_procesada]==id_dato_malo]
    df_datos_dudosos = datos_disponibles_bd[datos_disponibles_bd[qf_variable_procesada]==id_dato_dudoso]
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

        # Ordena según botellas
        datos_procesados = datos_procesados.sort_values('botella')

    
        ################# GRAFICOS ################
    
        ### GRAFICO CON LA VARIABLE ANALIZADA EN FUNCION DE LA PROFUNDIDAD Y OXIGENO   
        # Representa un gráfico con la variable seleccionada junto a los oxígenos 
        fig, (ax, az) = plt.subplots(1, 2, figsize=(20/2.54, 18/2.54), gridspec_kw = {'wspace':0.2, 'hspace':0}, width_ratios=[3, 1])
 
        ### DATOS DISPONIBLES PREVIAMENTE ###
        # Representa los datos con QF No evaluado si se seleccionó esta opción   
        if io_no_eval:
            ax.plot(df_datos_no_eval[variable_procesada],df_datos_no_eval['presion_ctd'],'.',color=color_no_eval,label='NO EVALUADO') 
        
        # Representa los datos con QF malos si se seleccionó esta opción   
        if io_malos:
            ax.plot(df_datos_malos[variable_procesada],df_datos_malos['presion_ctd'],'.',color=color_malos,label='MALO')    

        # Representa los datos con QF dudoso si se seleccionó esta opción   
        if io_dudosos:
            ax.plot(df_datos_dudosos[variable_procesada],df_datos_dudosos['presion_ctd'],'.',color=color_dudosos,label='DUDOSO')  

        # Representa los datos disponibles de un color
        if io_buenos:
            ax.plot(df_datos_buenos[variable_procesada],df_datos_buenos['presion_ctd'],'.',color=color_buenos,label='BUENO')
        
        # Representa los datos dentro del intervalo de meses en otro color
        if io_rango:
            ax.plot(df_rango_temporal[variable_procesada],df_rango_temporal['presion_ctd'],'.',color=color_rango,label='BUENO (INTERVALO)')
                
        ### DATOS PROCESADOS ###        
        ax.plot(datos_procesados[variable_procesada],datos_procesados['presion_ctd'],'.r',label='PROCESADO' )
        
        # ### FORMATO,ETIQUETAS Y NOMBRES DE EJES ###
        texto_eje = nombre_completo_variable_procesada + '(' + unidades_variable + ')'
        ax.set(xlabel=texto_eje)
        ax.set(ylabel='Presion (db)')
        ax.invert_yaxis()
        ax.set_xlim([vmin_rango, vmax_rango])
        rango_profs = ax.get_ylim()
        # Añade el nombre de cada punto
        nombre_muestreos = [None]*datos_procesados.shape[0]
        for ipunto in range(datos_procesados.shape[0]):     
            
            if datos_procesados[variable_procesada].iloc[ipunto] is not None:
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
            
            df_oxigeno = datos_procesados.loc[datos_procesados['oxigeno_ctd'].notnull(), ['oxigeno_ctd','presion_ctd']]            
            az.plot(df_oxigeno['oxigeno_ctd'],df_oxigeno['presion_ctd'],'.',color='#006633',label='OXIMETRO')
            #az.plot(datos_procesados['oxigeno_ctd'],datos_procesados['presion_ctd'],'.',color='#006633',label='OXIMETRO')
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
            fig, ax = plt.subplots(figsize=(18/2.54, 16/2.54))       
            
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
                
                if datos_procesados['nitrato'].iloc[ipunto] is not None and datos_procesados['fosfato'].iloc[ipunto] is not None:
                    ax.annotate(nombre_muestreos[ipunto], (datos_procesados['nitrato'].iloc[ipunto], datos_procesados['fosfato'].iloc[ipunto]))
    
            if not datos_procesados['ph'].isnull().all(): 
                if io_buenos:
                    az.plot(df_datos_buenos['nitrato'],df_datos_buenos['ph'],'.',color=color_buenos,label='BUENO')
                
                # Representa los datos dentro del intervalo de meses en otro color
                if io_rango:
                    az.plot(df_rango_temporal['nitrato'],df_rango_temporal['ph'],'.',color=color_rango,label='BUENO (INTERVALO)')
                
                # Representa los datos con QF malos si se seleccionó esta opción   
                if io_malos:
                    az.plot(df_datos_malos['nitrato'],df_datos_malos['ph'],'.',color=color_malos,label='MALO')    
    
                # Representa los datos con QF dudoso si se seleccionó esta opción   
                if io_dudosos:
                    az.plot(df_datos_dudosos['nitrato'],df_datos_dudosos['ph'],'.',color=color_dudosos,label='DUDOSO')    
                                      
                az.plot(datos_procesados['nitrato'],datos_procesados['ph'],'.r')
       
                az.set(xlabel='Nitrato (\u03BCmol/kg)')
                az.set(ylabel='pH')
                az.yaxis.tick_right()
                az.yaxis.set_label_position("right") 
                
                az.tick_params(axis='both', which='major', labelsize=8)
                az.xaxis.set_major_formatter(FormatStrFormatter('%.2f')) 
                az.yaxis.set_major_formatter(FormatStrFormatter('%.3f'))
                az.set_xlim([vmin_rango_x_g2, vmax_rango_x_g2])
                az.set_ylim([vmin_rango_y_g2, vmax_rango_y_g2])
            
                # Añade el nombre de cada punto
                nombre_muestreos = [None]*datos_procesados.shape[0]
                for ipunto in range(datos_procesados.shape[0]):
                    if datos_procesados['botella'].iloc[ipunto] is None:
                        nombre_muestreos[ipunto] = 'Prof.' + str(datos_procesados['presion_ctd'].iloc[ipunto])
                    else:
                        nombre_muestreos[ipunto] = 'Bot.' + str(int(datos_procesados['botella'].iloc[ipunto]))
                    
                    if datos_procesados['nitrato'].iloc[ipunto] is not None and datos_procesados['ph'].iloc[ipunto] is not None:
                        az.annotate(nombre_muestreos[ipunto], (datos_procesados['nitrato'].iloc[ipunto], datos_procesados['ph'].iloc[ipunto]))

            buf = BytesIO()
            fig.savefig(buf, format="png")
            st.image(buf)      
        
      
        
      
        
        
        # Gráficos particulares para cada variable
        elif variable_procesada == 'silicato':

            if not datos_procesados['alcalinidad'].isnull().all():              

                ### GRAFICO SILICATO vs ALCALINIDAD  
                
                with st.expander("Ajustar rango del gráfico SILICATO vs ALCALINIDAD",expanded=False):            
                    
                    st.write("Selecciona el rango del gráfico")  
                    
                    # Selecciona los rangos del gráfico
                    min_val_x,max_val_x = rango_datos(datos_procesados,datos_disponibles_bd,'silicato',df_indices_calidad,io_malos,io_dudosos,io_no_eval)
    
                    col1, col2, col3, col4 = st.columns(4,gap="small")
                    with col2:
                        vmin_rango_x  = st.number_input('Valor mínimo silicato:',value=min_val_x,key='vmin_sil')
                    with col3:
                        vmax_rango_x  = st.number_input('Valor máximo silicato:',value=max_val_x,key='vmax_sil')  
     
                    min_val_y,max_val_y = rango_datos(datos_procesados,datos_disponibles_bd,'alcalinidad',df_indices_calidad,io_malos,io_dudosos,io_no_eval)
    
                    col1, col2, col3, col4 = st.columns(4,gap="small")
                    with col2:
                        vmin_rango_y  = st.number_input('Valor mínimo alcalinidad:',value=min_val_y,key='vmin_alc')
                    with col3:
                        vmax_rango_y  = st.number_input('Valor máximo alcalinidad:',value=max_val_y,key='vmax_alc')   
    
    
    
                #fig, ax = plt.lots()       
                fig = plt.figure(figsize=(20/2.54, 18/2.54))
                ax = fig.add_subplot(111)
                
                if io_buenos:
                    plt.plot(df_datos_buenos['silicato'],df_datos_buenos['alcalinidad'],'.',color=color_buenos,label='BUENO')
                
                # Representa los datos dentro del intervalo de meses en otro color
                if io_rango:
                    plt.plot(df_rango_temporal['silicato'],df_rango_temporal['alcalinidad'],'.',color=color_rango,label='BUENO (INTERVALO)')
                
                # Representa los datos con QF malos si se seleccionó esta opción   
                if io_malos:
                    plt.plot(df_datos_malos['silicato'],df_datos_malos['alcalinidad'],'.',color=color_malos,label='MALO')    
    
                # Representa los datos con QF dudoso si se seleccionó esta opción   
                if io_dudosos:
                    plt.plot(df_datos_dudosos['silicato'],df_datos_dudosos['alcalinidad'],'.',color=color_dudosos,label='DUDOSO')    
                                
                
                plt.plot(datos_procesados['silicato'],datos_procesados['alcalinidad'],'.r' )
                
                ax.set(xlabel='Silicato (\u03BCmol/kg)')
                ax.set(ylabel='Alcalinidad (\u03BCmol/kg)')           
                 
                ax.tick_params(axis='both', which='major', labelsize=8)
                ax.xaxis.set_major_formatter(FormatStrFormatter('%.2f')) 
                ax.yaxis.set_major_formatter(FormatStrFormatter('%.0f'))
                ax.set_xlim([vmin_rango_x, vmax_rango_x])
                ax.set_ylim([vmin_rango_y, vmax_rango_y])
        
                # Añade el nombre de cada punto
                nombre_muestreos = [None]*datos_procesados.shape[0]
                for ipunto in range(datos_procesados.shape[0]):
                    if datos_procesados['botella'].iloc[ipunto] is None:
                        nombre_muestreos[ipunto] = 'Prof.' + str(datos_procesados['presion_ctd'].iloc[ipunto])
                    else:
                        nombre_muestreos[ipunto] = 'Bot.' + str(int(datos_procesados['botella'].iloc[ipunto]))
                    
                    if datos_procesados['silicato'].iloc[ipunto] is not None and datos_procesados['alcalinidad'].iloc[ipunto] is not None:
                        plt.annotate(nombre_muestreos[ipunto], (datos_procesados['silicato'].iloc[ipunto], datos_procesados['alcalinidad'].iloc[ipunto]))
                          
                buf = BytesIO()
                fig.savefig(buf, format="png")
                st.image(buf) 


    
        ################# FORMULARIOS CALIDAD ################        
    
        # Formulario para asignar banderas de calidad
        with st.form("Formulario", clear_on_submit=False):
                      
            indice_validacion = df_indices_calidad['indice'].tolist()
            texto_indice      = df_indices_calidad['descripcion'].tolist()
           
            for idato in range(datos_procesados.shape[0]):
                
                if nombre_muestreos[idato] is not None:
                    enunciado                                           = 'QF del muestreo ' + nombre_muestreos[idato]
                    valor_asignado                                      = st.radio(enunciado,texto_indice,horizontal=True,key = idato,index = 1)
                    datos_procesados[qf_variable_procesada].iloc[idato] = int(indice_validacion[texto_indice.index(valor_asignado)])
            
           
            io_envio = st.form_submit_button("Asignar los valores de QF seleccionados")  
    
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
                    instruccion_sql = "UPDATE datos_discretos SET " + qf_variable_procesada +  ' = %s WHERE muestreo = %s;'
           
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

def correccion_drift(datos_entrada,df_referencias_altas,df_referencias_bajas,variables_run,rendimiento_columna,temperatura_laboratorio):
    
    # Predimensiona un dataframe con los resultados de la correccion
    datos_corregidos = pandas.DataFrame(columns=variables_run)    

    # En runs con nitrato y nitrito calcular las concentraciones teniendo en cuenta el rendimiento
    if 'ton' in variables_run and 'nitrito' in variables_run:
    
        # Rendimiento de la columna del 100%, asignar directamente las concentraciones
        if rendimiento_columna == 100:
            datos_entrada['nitrato_rendimiento'] = datos_entrada['ton'] - datos_entrada['nitrito'] 
            datos_entrada['ton_rendimiento']     = datos_entrada['ton']       
        
        # Rendimiento menor al 100%, buscar los calibrantes y componer la concentración
        else:
        
            # Encuentra los índices (picos) correspondientes a la calbración
            indices_calibracion = numpy.asarray(datos_entrada['Peak Number'][datos_entrada['Cup Type']=='CALB']) - 1
                   
            # Corrige las concentraciones a partir de los rendimientos de la columna reductora
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
        if datos_entrada['Sample ID'].iloc[idato] is not None and datos_entrada['Sample ID'].iloc[idato][0:7].lower() == 'rmn low' :
            posicion_RMN_bajos[icont_bajos] = idato
            icont_bajos                     = icont_bajos + 1 
            datos_entrada['salinidad'].iloc[idato]  = df_referencias_bajas['salinidad'].iloc[0]
        if datos_entrada['Sample ID'].iloc[idato] is not None and datos_entrada['Sample ID'].iloc[idato][0:8].lower() == 'rmn high':
            posicion_RMN_altos[icont_altos] = idato
            icont_altos                     = icont_altos + 1
            datos_entrada['salinidad'].iloc[idato]  = df_referencias_altas['salinidad'].iloc[0]

    densidades = seawater.eos80.dens0(datos_entrada['salinidad'], datos_entrada['temp.lab'])
    datos_entrada['DENSIDAD'] = densidades/1000  
                    
    if 'ton' in variables_run:
        datos_entrada['ton_CONC'] = datos_entrada['ton_rendimiento']/datos_entrada['DENSIDAD']  
    if 'ton' in variables_run and 'nitrito' in variables_run:
        datos_entrada['nitrato_CONC'] = datos_entrada['nitrato_rendimiento']/datos_entrada['DENSIDAD']  
    if 'nitrito' in variables_run:
        datos_entrada['nitrito_CONC'] = datos_entrada['nitrito']/datos_entrada['DENSIDAD']  
    if 'silicato' in variables_run:
        datos_entrada['silicato_CONC'] = datos_entrada['silicato']/datos_entrada['DENSIDAD']  
    if 'fosfato' in variables_run:
        datos_entrada['fosfato_CONC'] = datos_entrada['fosfato']/datos_entrada['DENSIDAD']  


    
    ####  APLICA LA CORRECCIÓN DE DERIVA ####   
    for ivariable in range(len(variables_run)):
               
        variable_concentracion  = variables_run[ivariable] + '_CONC'
        
        # Concentraciones de las referencias 
        RMN_BAJO_variable = df_referencias_bajas[variables_run[ivariable]].iloc[0]
        RMN_ALTO_variable   = df_referencias_altas[variables_run[ivariable]].iloc[0]  
        
        # Concentraciones de las muestras analizadas como referencias
        RMN_altos       = datos_entrada[variable_concentracion][posicion_RMN_altos]
        RMN_bajos       = datos_entrada[variable_concentracion][posicion_RMN_bajos]
            
        # Predimensiona las rectas a y b
        indice_min_correccion = min(posicion_RMN_altos[0],posicion_RMN_bajos[0])
        indice_max_correccion = max(posicion_RMN_altos[1],posicion_RMN_bajos[1])
        recta_at              = numpy.zeros(datos_entrada.shape[0])
        recta_bt              = numpy.zeros(datos_entrada.shape[0])
            
        pte_RMN      = (RMN_ALTO_variable-RMN_BAJO_variable)/(RMN_altos.iloc[0]-RMN_bajos.iloc[0]) 
        t_indep_RMN  = RMN_BAJO_variable- pte_RMN*RMN_bajos.iloc[0] 
    
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
        
    # Corrige los valores negativos
    datos_corregidos[datos_corregidos<0] = 0
    
    # Añade columna con el identificador de cada muestra
    datos_corregidos['id_externo'] = datos_entrada['Sample ID']
       
    return datos_corregidos
    







################################################################
########### FUNCION PARA PROCESAR DATOS DE BOTELLAS ############
################################################################    

def procesado_botella(datos_botellas,id_estacion,nombre_estacion,id_programa,id_salida,tabla_estaciones_programa):
    
    datos_botellas['id_estacion_temp']         = id_estacion
    datos_botellas['estacion']                 = id_estacion
    datos_botellas['programa']                 = id_programa

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
                    
    if id_estacion == 2: #E3A
        listado_equiv_ctd = [1,3,5,7,9,11]
        listado_equiv_real = [13,131,132,133,134,135]
        for ibotella in range(datos_botellas.shape[0]):
            for iequiv in range(len(listado_equiv_ctd)):
                if datos_botellas['botella'].iloc[ibotella] == listado_equiv_ctd[iequiv]:
                    datos_botellas['botella'].iloc[ibotella] = listado_equiv_real[iequiv]

    if id_estacion == 3: #E3C
        listado_equiv_ctd = [1,3,5,7,9,11]
        listado_equiv_real = [14,141,142,143,144,145]
        for ibotella in range(datos_botellas.shape[0]):
            for iequiv in range(len(listado_equiv_ctd)):
                if datos_botellas['botella'].iloc[ibotella] == listado_equiv_ctd[iequiv]:
                    datos_botellas['botella'].iloc[ibotella] = listado_equiv_real[iequiv]
                    
    if id_estacion == 4: #E3B
        listado_equiv_ctd = [1,3,5,7,9,11]
        listado_equiv_real = [15,151,152,153,154,155]
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
  
    # Asigna lat/lon de la estación si esa información no etá incluia en el .btl
    for imuestreo in range(datos_botellas.shape[0]):
        if datos_botellas['latitud'].iloc[imuestreo] is None:
            datos_botellas['latitud'].iloc[imuestreo] = tabla_estaciones_programa['latitud_estacion'][tabla_estaciones_programa['id_estacion']==id_estacion].iloc[0]
        if datos_botellas['longitud'].iloc[imuestreo] is None:
            datos_botellas['longitud'].iloc[imuestreo] = tabla_estaciones_programa['longitud_estacion'][tabla_estaciones_programa['id_estacion']==id_estacion].iloc[0]
            
    # Asigna identificadores de salida al mar y estación
    datos_botellas['id_estacion_temp'] = datos_botellas['estacion']
    datos_botellas ['id_salida']       =  id_salida
    
    
    
    return datos_botellas



# ################################################################
# ########### FUNCION PARA PROCESAR DATOS DE PERFILES ############
# ################################################################    

def procesado_perfiles(datos_perfil,datos_muestreo_perfil,df_perfiles,id_salida,id_programa,abreviatura_programa,nombre_estacion,id_estacion,direccion_host,base_datos,usuario,contrasena,puerto):


    # Define el nombre del perfil
    nombre_perfil = abreviatura_programa + '_' + (datos_muestreo_perfil['fecha_muestreo'].iloc[0]).strftime("%Y%m%d") + '_E' + str(nombre_estacion) + '_C' + str(datos_muestreo_perfil['cast_muestreo'].iloc[0])
    
    # Conecta con la base de datos
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor() 
    
    # Obtén el identificador del perfil en la base de datos
    instruccion_sql = '''INSERT INTO perfiles_verticales (nombre_perfil,estacion,salida_mar,num_cast,fecha_perfil,hora_perfil,longitud_muestreo,latitud_muestreo)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_perfil,num_cast) DO NOTHING;''' 
    
    nombre_perfil = abreviatura_programa + '_' + (datos_muestreo_perfil['fecha_muestreo'].iloc[0]).strftime("%Y%m%d") + '_E' + str(nombre_estacion) + '_C' + str(datos_muestreo_perfil['cast_muestreo'].iloc[0])
    
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()    
    cursor.execute(instruccion_sql,(nombre_perfil,int(id_estacion),int(id_salida),int(datos_muestreo_perfil['cast_muestreo'].iloc[0]),datos_muestreo_perfil['fecha_muestreo'].iloc[0],datos_muestreo_perfil['hora_muestreo'].iloc[0],datos_muestreo_perfil['lon_muestreo'].iloc[0],datos_muestreo_perfil['lat_muestreo'].iloc[0]))
    conn.commit() 

    instruccion_sql = "SELECT perfil FROM perfiles_verticales WHERE nombre_perfil = '" + nombre_perfil + "';" 
    cursor = conn.cursor()    
    cursor.execute(instruccion_sql)
    id_perfil =cursor.fetchone()[0]
    conn.commit()       
    
    cursor.close()
    conn.close() 
    
    df_perfiles['perfil'] = int(id_perfil)
                                
    inserta_datos(df_perfiles,'perfil',direccion_host,base_datos,usuario,contrasena,puerto)
   
        
        
    if nombre_estacion == '2' and abreviatura_programa == 'RADCOR' :  # Estacion 2 del programa radiales, añadir muestreo correspondiente a la botella en superficie

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
              df_botella['oxigeno_ctd_qf']   = 2
          except:
              pass   
         
          df_botella['id_estacion_temp']       = int(id_estacion) 
          df_botella['id_salida']              = id_salida 
          df_botella['nombre_muestreo']        = abreviatura_programa + '_' + (datos_muestreo_perfil['fecha_muestreo'].iloc[0]).strftime("%Y%m%d") + '_E2_P0' 
         
          df_botella['programa']               = id_programa    
          df_botella['num_cast']               = datos_muestreo_perfil['cast_muestreo'].iloc[0]
         
          # Añade botella (7) y hora de muestreo (nulas) para evitar errores en el procesado
          df_botella['botella']                = 7
          df_botella['hora_muestreo']          = None
  
          # Añade qf 
          df_botella['temperatura_ctd_qf']     = 2
          df_botella['salinidad_ctd_qf']       = 2
          df_botella['fluorescencia_ctd_qf']   = 2
          df_botella['par_ctd_qf']             = 2
    
  
          df_botella                           = evalua_registros(df_botella,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
 
          inserta_datos(df_botella,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)
