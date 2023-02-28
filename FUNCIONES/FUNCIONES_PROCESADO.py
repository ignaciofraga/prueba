# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 08:08:39 2022

@author: ifraga
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
import seawater

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
        
        df_temporal = tabla_estaciones[tabla_estaciones['nombre_estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]]
    
        # Estacion ya incluida en la base de datos. Recuperar identificador
        if df_temporal.shape[0]>0:
            estaciones_muestreadas['id_estacion'][iestacion]       = df_temporal['id_estacion'].iloc[0]
            
        # Nueva estación, asignar orden creciente de identificador
        else:
            estaciones_muestreadas['id_estacion'][iestacion]       = id_ultima_estacion_bd + iconta_nueva_estacion
            estaciones_muestreadas['io_nueva_estacion'][iestacion] = 1           
            iconta_nueva_estacion                                 = iconta_nueva_estacion + 1
            
        # Asigna el identificador de estación a los datos importados
        datos['id_estacion_temp'][datos['estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]]=estaciones_muestreadas['id_estacion'][iestacion]
            
        # Determina la lat/lon de la estacion a partir de los valores de los registros asociados
        estaciones_muestreadas['latitud_estacion'][iestacion]  = (datos['latitud'][datos['estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]]).mean()
        estaciones_muestreadas['longitud_estacion'][iestacion] = (datos['longitud'][datos['estacion']==estaciones_muestreadas['nombre_estacion'][iestacion]]).mean()


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
        


            # instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
            # VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO UPDATE SET (nombre_salida,nombre_programa,tipo_salida,fecha_retorno,estaciones) = ROW(EXCLUDED.nombre_salida,EXCLUDED.nombre_programa,EXCLUDED.tipo_salida,EXCLUDED.fecha_retorno,EXCLUDED.estaciones);''' 

            instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
            VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO NOTHING;''' 
                               
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

    # RADIALES CORUÑA
    if id_programa == 3 :
    
        # Encuentra las fechas de muestreo únicas
        fechas_salidas_mar = datos['fecha_muestreo'].unique()
        
        # Asigna nombre a la salida
        meses = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']
           
        # Listado con los identificadores de buque en función de la configuración del perfilador
        id_buque              = [1,2,3,2]
        
        instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,buque,estaciones,configuracion_perfilador,configuracion_superficie)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO NOTHING;'''
            #UPDATE SET (nombre_salida,nombre_programa,tipo_salida,fecha_retorno,buque,estaciones) = ROW(EXCLUDED.nombre_salida,EXCLUDED.nombre_programa,EXCLUDED.tipo_salida,EXCLUDED.fecha_retorno,EXCLUDED.buque,EXCLUDED.estaciones);''' 
                
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
            
            id_configuracion_perfilador = int(list(subset_salida['configuracion_perfilador'].unique())[0])
            id_configuracion_superficie = int(list(subset_salida['configuracion_superficie'].unique())[0])
            if id_configuracion_perfilador:
                buque                       = int(id_buque[id_configuracion_perfilador-1])
            else:
                buque = None
          
            nombre_salida = nombre_programa + ' ' + tipo_salida + ' ' +   str(meses[fechas_salidas_mar[isalida].month-1]) + ' ' +  str(fechas_salidas_mar[isalida].year)
          
            cursor.execute(instruccion_sql, (nombre_salida,int(id_programa),nombre_programa,tipo_salida,fechas_salidas_mar[isalida],fechas_salidas_mar[isalida],buque,json_estaciones,id_configuracion_perfilador,id_configuracion_superficie))
            conn.commit()
            
            # Recupera el identificador de la salida al mar
            instruccion_sql_recupera = "SELECT id_salida FROM salidas_muestreos WHERE nombre_salida = '" + nombre_salida + "';"
            cursor.execute(instruccion_sql_recupera)
            id_salida =cursor.fetchone()[0]
            conn.commit()
    
            datos['id_salida'][datos['fecha_muestreo']==fechas_salidas_mar[isalida]] = id_salida
            
        cursor.close()
        conn.close()

    # RADIALES CANTABRICO
    if id_programa == 4 :
    
        # Extrae el mes y año de cada salida al mar
        datos['mes'] = numpy.zeros(datos.shape[0])
        datos['año'] = numpy.zeros(datos.shape[0])
        for idato in range(datos.shape[0]):
            datos['mes'].iloc[idato] =  datos['fecha_muestreo'].iloc[idato].month    
            datos['año'].iloc[idato] =  datos['fecha_muestreo'].iloc[idato].year 
    
    
        anhos_salida_mar = datos['año'].unique()

        
        # Asigna nombre a la salida
        meses = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']
           
        
        
        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
        cursor = conn.cursor()
        
        for ianho in range(len(anhos_salida_mar)):
        
            subset_anual     = datos[datos['año']==anhos_salida_mar[ianho]] 
            meses_salida_mar = subset_anual['mes'].unique()
        
            for isalida in range(len(meses_salida_mar)):
                
                # Encuentra las estaciones muestreadas
                subset_salida                        = subset_anual[subset_anual['mes']==meses_salida_mar[isalida]]
                identificador_estaciones_muestreadas = list(subset_salida['id_estacion_temp'].unique())
                estaciones_muestreadas               =[None]*len(identificador_estaciones_muestreadas)
                for iestacion in range(len(estaciones_muestreadas)):
                    estaciones_muestreadas[iestacion] = tabla_estaciones['nombre_estacion'][tabla_estaciones['id_estacion']==identificador_estaciones_muestreadas[iestacion]].iloc[0]
                json_estaciones        = json.dumps(estaciones_muestreadas)
                
                nombre_salida = nombre_programa + ' ' + tipo_salida + ' ' +   str(meses[int(meses_salida_mar[isalida]-1)]) + ' ' +  str(int(anhos_salida_mar[ianho]))
                
                if subset_salida['hora_muestreo'].unique()[0] is None:
                    
                    # instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
                    #     VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO UPDATE SET (nombre_salida,nombre_programa,tipo_salida,fecha_retorno,estaciones) = ROW(EXCLUDED.nombre_salida,EXCLUDED.nombre_programa,EXCLUDED.tipo_salida,EXCLUDED.fecha_retorno,EXCLUDED.estaciones);''' 

                    instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
                        VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO NOTHING;''' 
                            
                            
                    cursor.execute(instruccion_sql, (nombre_salida,int(id_programa),nombre_programa,tipo_salida,min(subset_salida['fecha_muestreo']),max(subset_salida['fecha_muestreo']),json_estaciones))

                else:
                    
                    # instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,hora_salida,hora_retorno,estaciones)
                    #     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO UPDATE SET (nombre_salida,nombre_programa,tipo_salida,fecha_retorno,hora_salida,hora_retorno,estaciones) = ROW(EXCLUDED.nombre_salida,EXCLUDED.nombre_programa,EXCLUDED.tipo_salida,EXCLUDED.fecha_retorno,EXCLUDED.hora_salida,EXCLUDED.hora_retorno,EXCLUDED.estaciones);''' 

                    instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,hora_salida,hora_retorno,estaciones)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO NOTHING;''' 
                          
                                    
                    cursor.execute(instruccion_sql, (nombre_salida,int(id_programa),nombre_programa,tipo_salida,min(subset_salida['fecha_muestreo']),max(subset_salida['fecha_muestreo']),min(subset_salida['hora_muestreo']),max(subset_salida['hora_muestreo']),json_estaciones))
                
                conn.commit()
                
                # Recupera el identificador de la salida al mar
                instruccion_sql_recupera = "SELECT id_salida FROM salidas_muestreos WHERE nombre_salida = '" + nombre_salida + "';"
                cursor.execute(instruccion_sql_recupera)
                #print(cursor.fetchall(),nombre_salida)
                id_salida =cursor.fetchone()[0]
                conn.commit()
        
                datos['id_salida'][(datos['año']==anhos_salida_mar[ianho]) & (datos['mes']==meses_salida_mar[isalida])] = id_salida
                
            
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
    
    
        # instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
        # VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO UPDATE SET (nombre_salida,nombre_programa,tipo_salida,fecha_retorno,estaciones) = ROW(EXCLUDED.nombre_salida,EXCLUDED.nombre_programa,EXCLUDED.tipo_salida,EXCLUDED.fecha_retorno,EXCLUDED.estaciones);''' 

        instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,fecha_retorno,estaciones)
        VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,fecha_salida) DO NOTHING;''' 

    
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
       
    datos['id_muestreo']  = numpy.zeros(datos.shape[0],dtype=int)
    
    # si no hay ningun valor en la tabla de registro, meter directamente todos los datos registrados
    if tabla_muestreos.shape[0] == 0:
    
        # genera un dataframe con las variables que interesa introducir en la base de datos
        exporta_registros                    = datos[['id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','latitud','longitud']]
        # añade el indice de cada registro
        indices_registros                    = numpy.arange(1,(exporta_registros.shape[0]+1))    
        exporta_registros['id_muestreo']     = indices_registros
        # renombra la columna con información de la estación muestreada
        exporta_registros                    = exporta_registros.rename(columns={"id_estacion_temp":"estacion",'id_salida':'salida_mar','latitud':'latitud_muestreo','longitud':'longitud_muestreo'})
        # # añade el nombre del muestreo
        exporta_registros['nombre_muestreo'] = [None]*exporta_registros.shape[0]
        for idato in range(exporta_registros.shape[0]):    
            nombre_estacion                              = tabla_estaciones.loc[tabla_estaciones['id_estacion'] == datos['id_estacion_temp'][idato]]['nombre_estacion'].iloc[0]
            
            nombre_muestreo     = abreviatura_programa + '_' + datos['fecha_muestreo'][idato].strftime("%Y%m%d") + '_E' + str(nombre_estacion)
            if datos['num_cast'][idato] is not None:
                nombre_muestreo = nombre_muestreo + '_C' + str(round(datos['num_cast'][idato]))
            else:
                nombre_muestreo = nombre_muestreo + '_C1' 
                
            if datos['botella'][idato] is not None:
                nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'][idato])) 
            else:
                if datos['prof_referencia'][idato] is not None: 
                    nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['prof_referencia'][idato]))
                else:
                    nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['presion_ctd'][idato])) 
                
            exporta_registros['nombre_muestreo'][idato]  = nombre_muestreo

            datos['id_muestreo'] [idato]                 = idato + 1
            
            
        # Inserta en base de datos        
        exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
        exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append') 
    
    # En caso contrario hay que ver registro a registro, si ya está incluido en la base de datos
    else:
    
        ultimo_registro_bd         = max(tabla_muestreos['id_muestreo'])
        datos['io_nuevo_muestreo'] = numpy.ones(datos.shape[0],dtype=int)

        for idato in range(datos.shape[0]):

            if datos['botella'][idato] is not None:        
                if datos['hora_muestreo'][idato] is not None:          
                    df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'][idato]) & (tabla_muestreos['botella']==datos['botella'][idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'][idato]) & (tabla_muestreos['hora_muestreo']==datos['hora_muestreo'][idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'][idato])]
    
                else:
                    df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'][idato]) & (tabla_muestreos['botella']==datos['botella'][idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'][idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'][idato])]
            else:
                if datos['hora_muestreo'][idato] is not None:          
                    df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'][idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'][idato]) & (tabla_muestreos['hora_muestreo']==datos['hora_muestreo'][idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'][idato])]
    
                else:
                    df_temp = tabla_muestreos[(tabla_muestreos['estacion']==datos['id_estacion_temp'][idato]) & (tabla_muestreos['fecha_muestreo']==datos['fecha_muestreo'][idato]) & (tabla_muestreos['presion_ctd']== datos['presion_ctd'][idato])]
               
            
            if df_temp.shape[0]> 0:
                datos['id_muestreo'] [idato]      =  df_temp['id_muestreo'].iloc[0]    
                datos['io_nuevo_muestreo'][idato] = 0
                
            else:
                datos['io_nuevo_muestreo'][idato] = 1
                ultimo_registro_bd                = ultimo_registro_bd + 1
                datos['id_muestreo'][idato]       = ultimo_registro_bd 
            
        
        if numpy.count_nonzero(datos['io_nuevo_muestreo']) > 0:
        
            # Genera un dataframe sólo con los valores nuevos, a incluir (io_nuevo_muestreo = 1)
            nuevos_muestreos  = datos[datos['io_nuevo_muestreo']==1]
            # Mantén sólo las columnas que interesan
            exporta_registros = nuevos_muestreos[['id_muestreo','id_estacion_temp','fecha_muestreo','hora_muestreo','id_salida','presion_ctd','prof_referencia','botella','num_cast','latitud','longitud']]
                        
            # Cambia el nombre de la columna de estaciones
            exporta_registros = exporta_registros.rename(columns={"id_estacion_temp":"estacion",'id_salida':'salida_mar','latitud':'latitud_muestreo','longitud':'longitud_muestreo'})
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
                else:
                    nombre_muestreo = nombre_muestreo + '_C1'     
                
                if datos['botella'][idato] is not None:
                    nombre_muestreo = nombre_muestreo + '_B' + str(round(datos['botella'][idato])) 
                else:
                    if datos['prof_referencia'][idato] is not None: 
                        nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['prof_referencia'][idato]))
                    else:
                        nombre_muestreo = nombre_muestreo + '_P' + str(round(datos['presion_ctd'][idato])) 
                 
                exporta_registros['nombre_muestreo'][idato]  = nombre_muestreo
        
            # # Inserta el dataframe resultante en la base de datos 
            exporta_registros.set_index('id_muestreo',drop=True,append=False,inplace=True)
            exporta_registros.to_sql('muestreos_discretos', conn_psql,if_exists='append')    
    
    conn_psql.dispose() # Cierra la conexión con la base de datos  
    
    return datos


##################################################################
######## FUNCION PARA INSERTAR DATOS EN LA BASE DE DATOS  ########
##################################################################

def inserta_datos(datos,tipo_datos,direccion_host,base_datos,usuario,contrasena,puerto):
  
    if tipo_datos     == 'fisica':
        variables     = 'variables_fisicas'
        tabla_destino = 'datos_discretos_fisica'
    
    elif tipo_datos   == 'bgq':
        variables     = 'variables_biogeoquimicas'  
        tabla_destino = 'datos_discretos_biogeoquimica'
        
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
    listado_variables_datos   = datos.columns.tolist()
    listado_variables_comunes = list(set(listado_variables_datos).intersection(variables_bd))
    listado_adicional         = ['muestreo'] + listado_variables_comunes
  
    # Genera un dataframe solo con las variales fisicas de los datos a importar 
    datos_insercion = datos.rename(columns={"id_muestreo": "muestreo"})
  
    # # Si no existe ningún registro en la base de datos, introducir todos los datos disponibles
    if tabla_registros.shape[0] == 0:
        
        datos_insercion = datos_insercion[listado_adicional]
        datos_insercion.set_index('muestreo',drop=True,append=False,inplace=True)
        datos_insercion.to_sql(tabla_destino, conn_psql,if_exists='append')
        
    # En caso contrario, comprobar qué parte de la información está en la base de datos
    else: 
        
        for idato in range(datos_insercion.shape[0]): # Dataframe con la interseccion de los datos nuevos y los disponibles en la base de datos, a partir de la variable muestreo
         
            df_temp  = tabla_registros[(tabla_registros['muestreo']==datos_insercion['muestreo'].iloc[idato])] 
            
            if df_temp.shape[0]>0:  # Muestreo ya incluido en la base de datos
            
                muestreo = df_temp['muestreo'].iloc[0]
                
                for ivariable in range(len(listado_variables_comunes)): # Reemplazar las variables disponibles en el muestreo correspondiente
                        
                    tabla_registros[listado_variables_comunes[ivariable]][tabla_registros['muestreo']==int(muestreo)] = datos_insercion[listado_variables_comunes[ivariable]][datos_insercion['muestreo']==int(muestreo)]
  
            
            else: # Nuevo muestreo
                       
                df_add = datos_insercion[datos_insercion['muestreo']==datos_insercion['muestreo'].iloc[idato]] # Genero un dataframe con cada línea de datos a añadir
  
                df_add = df_add[listado_adicional] # Recorto para que tenga sólo las variables a añadir
            
                tabla_registros = pandas.concat([tabla_registros, df_add]) # Combino ambos dataframes
            
        tabla_registros.set_index('muestreo',drop=True,append=False,inplace=True)
        
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
def control_calidad_biogeoquimica(datos_procesados,variables_procesado,variables_procesado_bd,variables_unidades):

    import streamlit as st
    import matplotlib.pyplot as plt
    from FUNCIONES.FUNCIONES_AUXILIARES import menu_seleccion   
    from FUNCIONES.FUNCIONES_AUXILIARES import init_connection 

    # Recupera los datos de conexión
    direccion_host   = st.secrets["postgres"].host
    base_datos       = st.secrets["postgres"].dbname
    usuario          = st.secrets["postgres"].user
    contrasena       = st.secrets["postgres"].password
    puerto           = st.secrets["postgres"].port

    # Recupera los datos disponibles en la base de datos
    conn                      = init_connection()
    df_muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
    df_datos_biogeoquimicos   = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn)
    df_datos_fisicos          = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn)
    df_indices_calidad        = psql.read_sql('SELECT * FROM indices_calidad', conn)
    conn.close()

    # Reemplaza los nan por None
    df_datos_biogeoquimicos   = df_datos_biogeoquimicos.replace(numpy.nan, None)
    df_datos_fisicos          = df_datos_fisicos.replace(numpy.nan, None)
    

    id_dato_malo              = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Malo'].iloc[0]
    id_dato_bueno             = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Bueno'].iloc[0]
    id_dato_dudoso            = df_indices_calidad['indice'][df_indices_calidad['descripcion']=='Dudoso'].iloc[0]


    listado_variables_fisicas       = df_datos_fisicos.columns.values.tolist()
    listado_variables_biogeoquimica = df_datos_biogeoquimicos.columns.values.tolist()
    listado_nutrientes              = ['ton','nitrato','nitrito','amonio','fosfato','silicato']
    
    ### CONTROL DE CALIDAD DE LOS DATOS

    # Despliega menú de selección del programa, año, salida, estación, cast y variable                 
    io_control_calidad = 1
    df_seleccion,indice_estacion,variable_seleccionada,salida_seleccionada,meses_offset = menu_seleccion(datos_procesados,variables_procesado,variables_procesado_bd,io_control_calidad)
    indice_variable = variables_procesado_bd.index(variable_seleccionada)

    qf_variable_seleccionada = variable_seleccionada + '_qf'

    # Recupera los datos disponibles de la misma estación, para la misma variable
    listado_muestreos_estacion = df_muestreos['id_muestreo'][df_muestreos['estacion']==indice_estacion]
    df_disponible_bgq_bd        = df_datos_biogeoquimicos[df_datos_biogeoquimicos['muestreo'].isin(listado_muestreos_estacion)]   
    df_disponible_bgq_bd        = df_disponible_bgq_bd.rename(columns={"muestreo": "id_muestreo"}) # Para igualar los nombres de columnas                                               
    df_disponible_bd            = pandas.merge(df_muestreos, df_disponible_bgq_bd, on="id_muestreo")

    df_disponible_fis_bd        = df_datos_fisicos[df_datos_fisicos['muestreo'].isin(listado_muestreos_estacion)]   
    df_disponible_fis_bd        = df_disponible_fis_bd.rename(columns={"muestreo": "id_muestreo"}) # Para igualar los nombres de columnas                                               
    df_disponible_bd            = pandas.merge(df_disponible_bd, df_disponible_fis_bd, on="id_muestreo")

    # Borra los dataframes que ya no hagan falta para ahorrar memoria
    del(df_datos_biogeoquimicos,df_datos_fisicos,df_muestreos,df_disponible_bgq_bd,df_disponible_fis_bd)

    # Genera un dataframe sólo con los datos "buenos"        
    df_datos_buenos = df_disponible_bd[df_disponible_bd[qf_variable_seleccionada]==id_dato_bueno]

    # comprueba si hay datos de la variable a analizar en la salida seleccionada
    if df_seleccion[variable_seleccionada].isnull().all():
        texto_error = "La base de datos no contiene información para la variable, salida y estación seleccionadas"
        st.warning(texto_error, icon="⚠️")

    elif df_datos_buenos[variable_seleccionada].isnull().all():
        texto_error = "La base de datos no contiene datos de la variable, salida y estación seleccionadas considerados como buenos"
        st.warning(texto_error, icon="⚠️")
    else:

        # Determina los meses que marcan el rango de busqueda
        df_seleccion    = df_seleccion.sort_values('fecha_muestreo')
        fecha_minima    = df_seleccion['fecha_muestreo'].iloc[0] - datetime.timedelta(days=meses_offset*30)
        fecha_maxima    = df_seleccion['fecha_muestreo'].iloc[-1] + datetime.timedelta(days=meses_offset*30)  
    
    
        if fecha_minima.year < fecha_maxima.year:
            listado_meses_1 = numpy.arange(fecha_minima.month,13)
            listado_meses_2 = numpy.arange(1,fecha_maxima.month+1)
            listado_meses   = numpy.concatenate((listado_meses_1,listado_meses_2))
        
        else:
            listado_meses   = numpy.arange(fecha_minima.month,fecha_maxima.month+1)
     
        listado_meses = listado_meses.tolist()
       
        # Genera un dataframe sólo con los datos "buenos"        
        df_datos_buenos = df_disponible_bd[df_disponible_bd[qf_variable_seleccionada]==id_dato_bueno]
                
        # Busca los datos de la base de datos dentro del rango de meses seleccionados
        df_datos_buenos['mes']  = pandas.DatetimeIndex(df_datos_buenos['fecha_muestreo']).month
        df_rango_temporal       = df_datos_buenos[df_datos_buenos['mes'].isin(listado_meses)]
                   
        # Líneas para separar un poco la parte gráfica de la de entrada de datos        
        for isepara in range(4):
            st.text(' ')
               
        with st.expander("Ajustar estilo de gráficos",expanded=False):
        
            st.write("Selecciona los datos a mostrar según su bandera de calidad")    
        
            # Selecciona mostrar o no datos malos y dudosos
            col1, col2, col3, col4 = st.columns(4,gap="small")
            with col1:
                io_buenos   = st.checkbox('Buenos', value=True)
                io_malos    = st.checkbox('Malos', value=False) 
            with col2:
                color_buenos = st.color_picker('Color', '#C0C0C0',label_visibility="collapsed")
                color_malos  = st.color_picker('Color', '#00CCCC',label_visibility="collapsed")
            with col3:
                io_rango      = st.checkbox('Buenos(intervalo)', value=True)
                io_dudosos    = st.checkbox('Dudosos', value=False)
            with col4:
                color_rango   = st.color_picker('Color', '#404040',label_visibility="collapsed")
                color_dudosos = st.color_picker('Color', '#00f900',label_visibility="collapsed")
            
        texto_rango = 'Ajustar rango del gráfico ' + variable_seleccionada.upper() + ' vs PROFUNDIDAD'
        with st.expander(texto_rango,expanded=False):            
            
            st.write("Selecciona el rango del gráfico")  
                 
            # df_seleccion             = df_seleccion.replace(numpy.nan, None) 
            # df_datos_buenos          = df_datos_buenos.replace(numpy.nan, None) 
            
            # Selecciona el rango del gráfico
            min_val = min(df_datos_buenos[variable_seleccionada].dropna().min(),df_seleccion[variable_seleccionada].dropna().min())
            max_val = max(df_datos_buenos[variable_seleccionada].dropna().max(),df_seleccion[variable_seleccionada].dropna().max())
            #min_val = min(min(df_datos_buenos[variable_seleccionada]),min(df_seleccion[variable_seleccionada]))
            #max_val = max(max(df_datos_buenos[variable_seleccionada]),max(df_seleccion[variable_seleccionada]))
   
            if io_malos:
                df_datos_malos = df_disponible_bd[df_disponible_bd[qf_variable_seleccionada]==id_dato_malo]
                min_val = min(min_val,df_datos_malos[variable_seleccionada].min())
            if io_dudosos:
                df_datos_dudosos = df_disponible_bd[df_disponible_bd[qf_variable_seleccionada]==id_dato_dudoso]
                min_val = min(min_val,df_datos_dudosos[variable_seleccionada].min())            

            rango   = (max_val-min_val)
            min_val = max(0,round(min_val - 0.025*rango,2))
            max_val = round(max_val + 0.025*rango,2)
            
            col1, col2, col3, col4 = st.columns(4,gap="small")
            with col2:
                vmin_rango  = st.number_input('Valor mínimo gráfico:',value=min_val,key='vmin_graf1')
            with col3:
                vmax_rango  = st.number_input('Valor máximo gráfico:',value=max_val,key='vmax_graf1')        


        ##Reemplaza nan por None
        #df_seleccion             = df_seleccion.replace(numpy.nan, None)            

    
        ################# GRAFICOS ################
    
        ### GRAFICO CON LA VARIABLE ANALIZADA EN FUNCION DE LA PROFUNDIDAD Y OXIGENO   
        # Representa un gráfico con la variable seleccionada junto a los oxígenos   
        fig, (ax, az) = plt.subplots(1, 2, gridspec_kw = {'wspace':0.2, 'hspace':0}, width_ratios=[3, 1])
 
        ### DATOS DISPONIBLES PREVIAMENTE ###
        # Representa los datos disponibles de un color
        if io_buenos:
            ax.plot(df_datos_buenos[variable_seleccionada],df_datos_buenos['presion_ctd'],'.',color=color_buenos,label='BUENO')
        
        # Representa los datos dentro del intervalo de meses en otro color
        if io_rango:
            ax.plot(df_rango_temporal[variable_seleccionada],df_rango_temporal['presion_ctd'],'.',color=color_rango,label='BUENO (INTERVALO)')
        
        # Representa los datos con QF malos si se seleccionó esta opción   
        if io_malos:
            ax.plot(df_datos_malos[variable_seleccionada],df_datos_malos['presion_ctd'],'.',color=color_malos,label='MALO')    

        # Representa los datos con QF dudoso si se seleccionó esta opción   
        if io_dudosos:
            ax.plot(df_datos_dudosos[variable_seleccionada],df_datos_dudosos['presion_ctd'],'.',color=color_dudosos,label='DUDOSO')    


        ### DATOS PROCESADOS ###        
        ax.plot(df_seleccion[variable_seleccionada],df_seleccion['presion_ctd'],'.r',label='PROCESADO' )
        
        ### FORMATO,ETIQUETAS Y NOMBRES DE EJES ###
        texto_eje = variables_procesado[indice_variable] + '(' + variables_unidades[indice_variable] + ')'
        ax.set(xlabel=texto_eje)
        ax.set(ylabel='Presion (db)')
        ax.invert_yaxis()
        #ax.set_xlim([vmin_rango, vmax_rango])
        rango_profs = ax.get_ylim()
        # Añade el nombre de cada punto
        nombre_muestreos = [None]*df_seleccion.shape[0]
        for ipunto in range(df_seleccion.shape[0]):            
            if df_seleccion['botella'].iloc[ipunto] is None:
                nombre_muestreos[ipunto] = 'Prof.' + str(int(df_seleccion['presion_ctd'].iloc[ipunto]))
            else:
                nombre_muestreos[ipunto] = 'Bot.' + str(int(df_seleccion['botella'].iloc[ipunto]))
            ax.annotate(nombre_muestreos[ipunto], (df_seleccion[variable_seleccionada].iloc[ipunto], df_seleccion['presion_ctd'].iloc[ipunto]))
                
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

            
        st.pyplot(fig)
 
    
 
        ### GRAFICOS ESPECIFICOS PARA LAS POSIBLES VARIABLES        

        

        if variable_seleccionada == 'fosfato':
            
            with st.expander("Ajustar rango del gráfico FOSFATO vs NITRATO",expanded=False):            
                
                st.write("Selecciona el rango del gráfico")  
                
                # Selecciona los rangos del gráfico
                min_val_x = 0.95*min(df_disponible_bd['nitrato'].min(),df_seleccion['nitrato'].min())
                max_val_x = 1.05*max(df_disponible_bd['nitrato'].max(),df_seleccion['nitrato'].max())
                   
                col1, col2, col3, col4 = st.columns(4,gap="small")
                with col2:
                    vmin_rango_x  = st.number_input('Valor mínimo eje x:',value=min_val_x,key='vmin_x_graf_fosf')
                with col3:
                    vmax_rango_x  = st.number_input('Valor máximo eje x:',value=max_val_x,key='vmax_x_graf_fosf')  
 
                min_val_y = 0.95*min(df_disponible_bd['fosfato'].min(),df_seleccion['fosfato'].min())
                max_val_y = 1.05*max(df_disponible_bd['fosfato'].max(),df_seleccion['fosfato'].max())
                   
                col1, col2, col3, col4 = st.columns(4,gap="small")
                with col2:
                    vmin_rango_y  = st.number_input('Valor mínimo eje y:',value=min_val_y,key='vmin_y_graf_fosf')
                with col3:
                    vmax_rango_y  = st.number_input('Valor máximo eje y:',value=max_val_y,key='vmax_y_graf_fosf') 
            
            
    
            ### GRAFICO FOSFATO vs NITRATO 
            fig, ax = plt.subplots()       
            
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
                                  
            ax.plot(df_seleccion['nitrato'],df_seleccion['fosfato'],'.r' )

            ax.set(xlabel='Nitrato (\u03BCmol/kg)')
            ax.set(ylabel='Fosfato (\u03BCmol/kg)')
    

            # Reduce el tamaño y ajusta el rango y formato de los ejes
            ax.tick_params(axis='both', which='major', labelsize=8)
            ax.set_xlim([vmin_rango_x, vmax_rango_x])
            ax.set_ylim([vmin_rango_y, vmax_rango_y])
            ax.xaxis.set_major_formatter(FormatStrFormatter('%.2f'))
            ax.yaxis.set_major_formatter(FormatStrFormatter('%.2f')) 
    
    
    
            # Añade el nombre de cada punto
            nombre_muestreos = [None]*df_seleccion.shape[0]
            for ipunto in range(df_seleccion.shape[0]):
                if df_seleccion['botella'].iloc[ipunto] is None:
                    nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
                else:
                    nombre_muestreos[ipunto] = 'Bot.' + str(int(df_seleccion['botella'].iloc[ipunto]))
                ax.annotate(nombre_muestreos[ipunto], (df_seleccion['nitrato'].iloc[ipunto], df_seleccion['fosfato'].iloc[ipunto]))
           
            st.pyplot(fig)
        
        





        
        elif variable_seleccionada == 'nitrato':
    
            with st.expander("Ajustar rango del gráfico NITRATO vs FOSFATO",expanded=False):            
                
                st.write("Selecciona el rango del gráfico")  
                
                # Selecciona los rangos del gráfico
                min_val_x = 0.95*min(df_disponible_bd['nitrato'].min(),df_seleccion['nitrato'].min())
                max_val_x = 1.05*max(df_disponible_bd['nitrato'].max(),df_seleccion['nitrato'].max())
                   
                col1, col2, col3, col4 = st.columns(4,gap="small")
                with col2:
                    vmin_rango_x  = st.number_input('Valor mínimo nitrato:',value=min_val_x,key='vmin_nit')
                with col3:
                    vmax_rango_x  = st.number_input('Valor máximo nitrato:',value=max_val_x,key='vmax_nit')  
 
                min_val_y = 0.95*min(df_disponible_bd['fosfato'].min(),df_seleccion['fosfato'].min())
                max_val_y = 1.05*max(df_disponible_bd['fosfato'].max(),df_seleccion['fosfato'].max())
                   
                col1, col2, col3, col4 = st.columns(4,gap="small")
                with col2:
                    vmin_rango_y  = st.number_input('Valor mínimo fosfato:',value=min_val_y,key='vmin_fosf')
                with col3:
                    vmax_rango_y  = st.number_input('Valor máximo fosfato:',value=max_val_y,key='vmax_fosf')         

   
                    
            if df_seleccion['ph'].isnull().all():         
                fig, ax = plt.subplots()      
            else:
                
                with st.expander("Ajustar rango del gráfico NITRATO vs ph",expanded=False):            
                    
                    st.write("Selecciona el rango del gráfico")  
                    
                    # Selecciona los rangos del gráfico
                    min_val_x_g2 = 0.95*min(df_disponible_bd['nitrato'].min(),df_seleccion['nitrato'].min())
                    max_val_x_g2 = 1.05*max(df_disponible_bd['nitrato'].max(),df_seleccion['nitrato'].max())
                       
                    col1, col2, col3, col4 = st.columns(4,gap="small")
                    with col2:
                        vmin_rango_x_g2  = st.number_input('Valor mínimo nitrato:',value=min_val_x_g2,key='vmin_nit_2')
                    with col3:
                        vmax_rango_x_g2  = st.number_input('Valor máximo nitrato:',value=max_val_x_g2,key='vmax_nit_2')  
     
                    min_val_y_g2 = 0.95*min(df_disponible_bd['ph'].min(),df_seleccion['ph'].min())
                    max_val_y_g2 = 1.05*max(df_disponible_bd['ph'].max(),df_seleccion['ph'].max())
                       
                    col1, col2, col3, col4 = st.columns(4,gap="small")
                    with col2:
                        vmin_rango_y_g2  = st.number_input('Valor mínimo pH:',value=min_val_y_g2,key='vmin_ph')
                    with col3:
                        vmax_rango_y_g2  = st.number_input('Valor máximo pH:',value=max_val_y_g2,key='vmax_ph') 

                fig, (ax, az) = plt.subplots(1, 2, gridspec_kw = {'wspace':0.1, 'hspace':0}, width_ratios=[1, 1])      
    
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
  
            ax.plot(df_seleccion['nitrato'],df_seleccion['fosfato'],'.r' )
            
            ax.set(xlabel='Nitrato (\u03BCmol/kg)')
            ax.set(ylabel='Fosfato (\u03BCmol/kg)')
            
            # Reduce el tamaño y ajusta el formato de los ejes
            ax.tick_params(axis='both', which='major', labelsize=8)
            ax.xaxis.set_major_formatter(FormatStrFormatter('%.2f')) 
            ax.yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
            ax.set_xlim([vmin_rango_x, vmax_rango_x])
            ax.set_ylim([vmin_rango_y, vmax_rango_y])
    
            # Añade el nombre de cada punto
            nombre_muestreos = [None]*df_seleccion.shape[0]
            for ipunto in range(df_seleccion.shape[0]):
                if df_seleccion['botella'].iloc[ipunto] is None:
                    nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
                else:
                    nombre_muestreos[ipunto] = 'Bot.' + str(int(df_seleccion['botella'].iloc[ipunto]))
                ax.annotate(nombre_muestreos[ipunto], (df_seleccion['nitrato'].iloc[ipunto], df_seleccion['fosfato'].iloc[ipunto]))
    
            
            if df_seleccion['ph'].isnull().all() is False: 
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
                                      
        
                az.plot(df_disponible_bd['nitrato'],df_disponible_bd['ph'],'.',color='#C0C0C0')
     
        
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
                nombre_muestreos = [None]*df_seleccion.shape[0]
                for ipunto in range(df_seleccion.shape[0]):
                    if df_seleccion['botella'].iloc[ipunto] is None:
                        nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
                    else:
                        nombre_muestreos[ipunto] = 'Bot.' + str(int(df_seleccion['botella'].iloc[ipunto]))
                    az.annotate(nombre_muestreos[ipunto], (df_seleccion['nitrato'].iloc[ipunto], df_seleccion['ph'].iloc[ipunto]))
         
    
            st.pyplot(fig)
      
        
      
        
      
        
        
        # Gráficos particulares para cada variable
        elif variable_seleccionada == 'silicato':
    
            if df_seleccion['silicato'].isnull().all() is False:         
    
                ### GRAFICO SILICATO vs ALCALINIDAD  
                
                with st.expander("Ajustar rango del gráfico SILICATO vs ALCALINIDAD",expanded=False):            
                    
                    st.write("Selecciona el rango del gráfico")  
                    
                    # Selecciona los rangos del gráfico
                    min_val_x = 0.95*min(df_disponible_bd['silicato'].min(),df_seleccion['silicato'].min())
                    max_val_x = 1.05*max(df_disponible_bd['silicato'].max(),df_seleccion['silicato'].max())
                       
                    col1, col2, col3, col4 = st.columns(4,gap="small")
                    with col2:
                        vmin_rango_x  = st.number_input('Valor mínimo silicato:',value=min_val_x,key='vmin_sil')
                    with col3:
                        vmax_rango_x  = st.number_input('Valor máximo silicato:',value=max_val_x,key='vmax_sil')  
     
                    min_val_y = 0.95*min(df_disponible_bd['alcalinidad'].min(),df_seleccion['alcalinidad'].min())
                    max_val_y = 1.05*max(df_disponible_bd['alcalinidad'].max(),df_seleccion['alcalinidad'].max())
                       
                    col1, col2, col3, col4 = st.columns(4,gap="small")
                    with col2:
                        vmin_rango_y  = st.number_input('Valor mínimo alcalinidad:',value=min_val_y,key='vmin_alc')
                    with col3:
                        vmax_rango_y  = st.number_input('Valor máximo alcalinidad:',value=max_val_y,key='vmax_alc')   
    
                fig, ax = plt.subplots()       
                
                if io_buenos:
                    ax.plot(df_datos_buenos['silicato'],df_datos_buenos['alcalinidad'],'.',color=color_buenos,label='BUENO')
                
                # Representa los datos dentro del intervalo de meses en otro color
                if io_rango:
                    ax.plot(df_rango_temporal['silicato'],df_rango_temporal['alcalinidad'],'.',color=color_rango,label='BUENO (INTERVALO)')
                
                # Representa los datos con QF malos si se seleccionó esta opción   
                if io_malos:
                    ax.plot(df_datos_malos['silicato'],df_datos_malos['alcalinidad'],'.',color=color_malos,label='MALO')    
    
                # Representa los datos con QF dudoso si se seleccionó esta opción   
                if io_dudosos:
                    ax.plot(df_datos_dudosos['silicato'],df_datos_dudosos['alcalinidad'],'.',color=color_dudosos,label='DUDOSO')    
                                
                
                ax.plot(df_seleccion['silicato'],df_seleccion['alcalinidad'],'.r' )
                
                
                ax.set(xlabel='Silicato (\u03BCmol/kg)')
                ax.set(ylabel='Alcalinidad (\u03BCmol/kg)')
                
                ax.tick_params(axis='both', which='major', labelsize=8)
                ax.xaxis.set_major_formatter(FormatStrFormatter('%.2f')) 
                ax.yaxis.set_major_formatter(FormatStrFormatter('%.0f'))
                ax.set_xlim([vmin_rango_x, vmax_rango_x])
                ax.set_ylim([vmin_rango_y, vmax_rango_y])
        
                # Añade el nombre de cada punto
                nombre_muestreos = [None]*df_seleccion.shape[0]
                for ipunto in range(df_seleccion.shape[0]):
                    if df_seleccion['botella'].iloc[ipunto] is None:
                        nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
                    else:
                        nombre_muestreos[ipunto] = 'Bot.' + str(int(df_seleccion['botella'].iloc[ipunto]))
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
    
                    if variable_seleccionada in listado_variables_fisicas:
                        instruccion_sql = "UPDATE datos_discretos_fisica SET " + variable_seleccionada + ' = %s, ' + variable_seleccionada +  '_qf = %s WHERE muestreo = %s;'
                        cursor.execute(instruccion_sql, (df_seleccion[variable_seleccionada].iloc[idato],int(qf_asignado[idato]),int(df_seleccion['muestreo'].iloc[idato])))
                
                    if variable_seleccionada in listado_variables_biogeoquimica: 
                        if variable_seleccionada in listado_nutrientes:
                            instruccion_sql = "UPDATE datos_discretos_biogeoquimica SET " + variable_seleccionada + ' = %s, ' + variable_seleccionada +  '_qf = %s, cc_nutrientes = %s WHERE muestreo = %s;'
                            cursor.execute(instruccion_sql, (df_seleccion[variable_seleccionada].iloc[idato],int(qf_asignado[idato]),int(1),int(df_seleccion['muestreo'].iloc[idato])))
                                    
                        else:
                            instruccion_sql = "UPDATE datos_discretos_biogeoquimica SET " + variable_seleccionada + ' = %s, ' + variable_seleccionada +  '_qf = %s WHERE muestreo = %s;'
                            cursor.execute(instruccion_sql, (df_seleccion[variable_seleccionada].iloc[idato],int(qf_asignado[idato]),int(df_seleccion['muestreo'].iloc[idato])))
                                 
                    conn.commit() 
    
                cursor.close()
                conn.close()   
    
            texto_exito = 'Datos de salida ' + salida_seleccionada + ' añadidos o modificados correctamente'
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