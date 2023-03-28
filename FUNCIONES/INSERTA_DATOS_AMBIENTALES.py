# -*- coding: utf-8 -*-
"""
Created on Mon Nov 21 11:56:00 2022

@author: ifraga
"""
import pandas
import pandas.io.sql as psql
from sqlalchemy import create_engine
import psycopg2
import numpy


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# recupera la información de los muestreos incluidos en la base de datos
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
df_salidas       = psql.read_sql('SELECT * FROM salidas_muestreos', conn_psql)
df_estaciones    = psql.read_sql('SELECT * FROM estaciones', conn_psql)
df_condiciones   = psql.read_sql('SELECT * FROM condiciones_ambientales_muestreos', conn_psql)



archivo_condiciones = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/AMBIENTALES/AMBIENTALES.xlsx'

datos_ambientales   = pandas.read_excel(archivo_condiciones, 'condiciones ambientales')

# Quita la primera fila, con datos de unidades
datos_ambientales = datos_ambientales.iloc[1: , :]

# Convierte las fechas al formato correspondiente
for idato in range(datos_ambientales.shape[0]):
    datos_ambientales['Fecha '].iloc[idato] = datos_ambientales['Fecha '].iloc[idato].date()
         
# Cambia los nans por None
datos_ambientales = datos_ambientales.replace(numpy.nan, None)
    
# Une con el df de las salidas, para que ya se queden sólo los datos que tienen una salida asociada    
datos_ambientales = datos_ambientales.rename(columns={"Fecha ": "fecha_salida"}) # Para igualar los nombres de columnas                                               
datos_ambientales = pandas.merge(datos_ambientales, df_salidas, on="fecha_salida")

# Encuentra la estación asignada
datos_ambientales['id_estacion'] = numpy.zeros(datos_ambientales.shape[0],dtype=int)
for idato in range(datos_ambientales.shape[0]):
    datos_ambientales['id_estacion'].iloc[idato] = df_estaciones['id_estacion'][df_estaciones['nombre_estacion']==str(datos_ambientales['Estación'].iloc[idato]).upper()].iloc[0]

# Ajusta el texto de lluvia
for idato in range(datos_ambientales.shape[0]):
    if datos_ambientales['Lluvia'].iloc[idato] == 'si' or datos_ambientales['Lluvia'].iloc[idato] == 'SI' or datos_ambientales['Lluvia'].iloc[idato] == 'Si' or datos_ambientales['Lluvia'].iloc[idato] == 'sI':
        datos_ambientales['Lluvia'].iloc[idato] = 'Si'
    elif datos_ambientales['Lluvia'].iloc[idato] == 'no' or datos_ambientales['Lluvia'].iloc[idato] == 'NO' or datos_ambientales['Lluvia'].iloc[idato] == 'No' or datos_ambientales['Lluvia'].iloc[idato] == 'nO':
        datos_ambientales['Lluvia'].iloc[idato] = 'No'
    else:
        datos_ambientales['Lluvia'].iloc[idato] = None
    
datos_ambientales['Douglas']  = None
douglas_nombre = ['Mar rizada (1)','Marejadilla (2)', 'Marejada (3)', 'Fuerte marejada (4)', 'Gruesa (5)', 'Muy Gruesa (6)']
douglas_vmin   = [0  , 0.1 , 0.50 , 1.25, 2.5, 4]
douglas_vmax   = [0.1, 0.5 , 1.25 , 2.50, 4.0, 6]

for idato in range(datos_ambientales.shape[0]):   
    if datos_ambientales['Altura de Ola'].iloc[idato] is not None:
        for iescala in range(len(douglas_nombre)) :
            if datos_ambientales['Altura de Ola'].iloc[idato] >= douglas_vmin [iescala] and datos_ambientales['Altura de Ola'].iloc[idato] < douglas_vmax [iescala]:
                datos_ambientales['Douglas'].iloc[idato] = douglas_nombre[iescala]
            if datos_ambientales['Altura de Ola'].iloc[idato] < douglas_vmin [0]:
                datos_ambientales['Douglas'].iloc[idato] = douglas_nombre[0]
            if datos_ambientales['Altura de Ola'].iloc[idato] >= douglas_vmin [-1]:
                datos_ambientales['Douglas'].iloc[idato] = douglas_nombre[-1]

# Recorta las variables que interesan
datos_exporta = datos_ambientales[['id_salida','id_estacion','Hora inicio','Profundidad','Nubosidad','Lluvia','Viento','Viento_dir','Presíon','Altura de Ola','Mar de fondo','Douglas','Mar','Humedad','Tª aire','Secchi','Max. Cla','Marea','Tª superf.']]
                                                                                                                                         
# Cambia los nombres de las variables
datos_exporta = datos_exporta.rename(columns={"id_salida": "salida", "id_estacion": "estacion", 'Hora inicio':'hora_llegada','Profundidad':'profundidad',
                                                 'Nubosidad':'nubosidad','Lluvia':'lluvia','Viento':'velocidad_viento','Viento_dir':'direccion_viento',
                                                 'Presíon':'pres_atmosferica','Altura de Ola':'altura_ola','Mar de fondo':'mar_fondo','Douglas':'estado_mar',
                                                 'Mar':'mar_direccion','Humedad':'humedad_relativa','Tª aire':'temp_aire','Secchi':'prof_secchi','Max. Cla':'max_clorofila',
                                                 'Marea':'marea','Tª superf.':'temp_superficie'}) 


if df_condiciones.shape[0] == 0:

    # Recorta el dataframe para tener sólo las estaciones del programa seleccionado
    indices_dataframe              = numpy.arange(1,datos_exporta.shape[0]+1,1,dtype=int)    
    datos_exporta['id_condicion'] = indices_dataframe
    datos_exporta.set_index('id_condicion',drop=True,append=False,inplace=True)    
    
    # Inserta el dataframe resultante en la base de datos 
    datos_exporta.to_sql('condiciones_ambientales_muestreos', conn_psql,if_exists='append')

else:
    
    datos_exporta['id_condicion'] = [None]*datos_exporta.shape[0]
    listado_variables = datos_exporta.columns.tolist()
    ultimo_registro_bd         = max(df_condiciones['id_condicion'])
    
    for idato in range(datos_exporta.shape[0]):
        
        df_temp = df_condiciones[(df_condiciones['salida']==datos_exporta['salida'].iloc[idato]) & (df_condiciones['estacion']==datos_exporta['estacion'].iloc[idato])]

        if df_temp.shape[0]>0:  # Muestreo ya incluido en la base de datos
        
            condicion = df_temp['id_condicion'].iloc[0]
                            
            for ivariable in range(len(listado_variables)): # Reemplazar las variables disponibles en el muestreo correspondiente
                    
                df_condiciones[listado_variables[ivariable]][df_condiciones['id_condicion']==int(condicion)] = datos_exporta[listado_variables[ivariable]].iloc[idato]

        else: # Nuevo muestreo
                   
            df_add = datos_exporta.iloc[idato] # Genero un dataframe con cada línea de datos a añadir
  
            df_add['id_condicion'] = ultimo_registro_bd + 1
            ultimo_registro_bd     = ultimo_registro_bd + 1 
        
            df_condiciones = pandas.concat([df_condiciones, df_add]) # Combino ambos dataframes

    indices_dataframe              = numpy.arange(1,df_condiciones.shape[0]+1,1,dtype=int)    
    df_condiciones['id_condicion'] = indices_dataframe
    df_condiciones.set_index('id_condicion',drop=True,append=False,inplace=True) 

    # borra los registros existentes en la tabla (no la tabla en sí, para no perder tipos de datos y referencias)
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()
    instruccion_sql = "TRUNCATE condiciones_ambientales_muestreos;"
    cursor.execute(instruccion_sql)
    conn.commit()
    cursor.close()
    conn.close() 
        
    # Inserta el dataframe resultante en la base de datos 
    df_condiciones.to_sql('condiciones_ambientales_muestreos', conn_psql,if_exists='replace')


conn_psql.dispose()
# instruccion_sql_introduccion = '''INSERT INTO condiciones_ambientales_muestreos (salida,estacion,hora_llegada,profundidad,nubosidad,lluvia,velocidad_viento,direccion_viento,pres_atmosferica,altura_ola,mar_fondo,estado_mar,mar_direccion,humedad_relativa,temp_aire,prof_secchi,max_clorofila,marea,temp_superficie)
#     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (salida,estacion) DO UPDATE SET (hora_llegada,profundidad,nubosidad,lluvia,velocidad_viento,direccion_viento,pres_atmosferica,altura_ola,mar_fondo,estado_mar,mar_direccion,humedad_relativa,temp_aire,prof_secchi,max_clorofila,marea,temp_superficie) = ROW(EXCLUDED.hora_llegada,EXCLUDED.profundidad,EXCLUDED.nubosidad,EXCLUDED.lluvia,EXCLUDED.velocidad_viento,EXCLUDED.direccion_viento,EXCLUDED.pres_atmosferica,EXCLUDED.altura_ola,EXCLUDED.mar_fondo,EXCLUDED.estado_mar,EXCLUDED.mar_direccion,EXCLUDED.humedad_relativa,EXCLUDED.temp_aire,EXCLUDED.prof_secchi,EXCLUDED.max_clorofila,EXCLUDED.marea,EXCLUDED.temp_superficie);''' 

# instruccion_sql_consulta = '''SELECT id_condicion FROM condiciones_ambientales_muestreos WHERE salida = %s and estacion = %s ''' 
        

conn   = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()     
    
# # Introduce datos en la base de datos
# for idato in range(datos_ambientales.shape[0]):          
    
    cursor.execute(instruccion_sql_consulta, (int(datos_ambientales['id_salida'].iloc[idato]),int(datos_ambientales['id_estacion'].iloc[idato])))
    id_condicion = cursor.fetchone()[]
    
#     cursor.execute(instruccion_sql, (int(datos_ambientales['id_salida'].iloc[idato]),int(datos_ambientales['id_estacion'].iloc[idato]),datos_ambientales['Hora inicio'].iloc[idato],datos_ambientales['Profundidad'].iloc[idato],datos_ambientales['Nubosidad'].iloc[idato],datos_ambientales['Lluvia'].iloc[idato],datos_ambientales['Viento'].iloc[idato],datos_ambientales['Viento_dir'].iloc[idato],datos_ambientales['Presíon'].iloc[idato],datos_ambientales['Altura de Ola'].iloc[idato],datos_ambientales['Mar de fondo'].iloc[idato],datos_ambientales['Douglas'].iloc[idato],datos_ambientales['Mar'].iloc[idato],datos_ambientales['Humedad'].iloc[idato],datos_ambientales['Tª aire'].iloc[idato],datos_ambientales['Secchi'].iloc[idato],datos_ambientales['Max. Cla'].iloc[idato],datos_ambientales['Marea'].iloc[idato],datos_ambientales['Tª superf.'].iloc[idato]))
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        

#     conn.commit()
# cursor.close()
# conn.close()

# if io_previo == 0:
#     texto_exito = 'Datos de las estación ' + estacion_elegida + ' durante la salida '  + salida  + ' añadidos correctamente'
# if io_previo == 1:
#     texto_exito = 'Datos de las estación ' + estacion_elegida + ' durante la salida '  + salida  + ' actualizados correctamente'
    
# st.success(texto_exito)                

